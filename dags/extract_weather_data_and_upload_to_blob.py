from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import logging
from io import BytesIO

# Constants
AZURE_CONTAINER_NAME = "weather-data"
LOCATIONS = [
    {"lat": "54.6778816", "lon": "-5.9249199"},
    {"lat": "52.6362", "lon": "-1.1331969"},
    {"lat": "51.456659", "lon": "-0.9696512"},
    {"lat": "54.1775283", "lon": "-6.337506"},
    {"lat": "51.4867", "lon": "0.2433"},
    {"lat": "53.4071991", "lon": "-2.99168"},
    {"lat": "53.3045372", "lon": "-1.1028469453936067"},
    {"lat": "55.9007", "lon": "-3.5181"},
    {"lat": "53.5227681", "lon": "-1.1335312"},
    {"lat": "52.802742", "lon": "-1.629917"},
]


def extract_weather_data(execution_date, **kwargs):
    """
    Extracts weather data from OpenWeather API and stores it in pandas dataframe
    And pushes it to XCom for downstream tasks

    Parameters
    ----------
    execution_date : datetime
        Execution date of the DAG

    Raises
    ------
    ValueError
        If the weather data retrieval fails
    """
    api_key = Variable.get("openweather_api_key")
    logging.info(f"Extracting weather data for execution date: {execution_date}")
    timestamp = datetime.utcnow()  # current extraction time

    all_data = []
    # loop through locations and retrieve weather data
    for loc in LOCATIONS:
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                "lat": loc["lat"],
                "lon": loc["lon"],
                "appid": api_key,
            },
        )

        if response.status_code != 200:
            logging.warning(
                f"Failed to fetch weather for {loc}, status code {response.status_code}"
            )
            continue

        data = response.json()
        data["extraction_timestamp"] = timestamp.isoformat()
        all_data.append(data)

    # If there is no data at all, Raise an error
    if not all_data:
        raise ValueError("No valid weather data retrieved.")

    # Convert the list of dictionaries to a pandas dataframe
    df = pd.DataFrame(all_data)
    logging.info(f"Extracted weather data for {len(df)} locations")
    logging.info(f"Dataframe head: {df.head()}")

    # Push to XCom for downstream tasks
    kwargs["ti"].xcom_push(key="weather_parquet_df", value=df)
    kwargs["ti"].xcom_push(
        key="timestamp_str", value=timestamp.strftime("%Y-%m-%dT%H-%M-%S")
    )


def upload_to_blob(execution_date, **kwargs):
    """
    Pulls the weather data from XCom and uploads it to Azure Blob Storage

    Parameters
    ----------
    execution_date : datetime
        Execution date of the DAG

    """
    try:
        # Pull the weather data from XCom
        weather_parquet_df = kwargs["ti"].xcom_pull(
            task_ids="extract_weather", key="weather_parquet_df"
        )
        # Pull the timestamp from XCom
        timestamp_str = kwargs["ti"].xcom_pull(
            task_ids="extract_weather", key="timestamp_str"
        )

        # Pull the connection string from Key Vault
        conn_str = Variable.get("azure_blob_conn_str")

        # The parquet file is stored with partitions in the format of "date=YYYY-MM-DD/weather_data_{timestamp}.parquet"
        # The partitions are used to store the data in a partitioned manner, which allows for faster querying
        blob_name = f"date={execution_date.date()}/weather_data_{timestamp_str}.parquet"

        # Create a BlobServiceClient object from the connection string
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, blob=blob_name
        )

        # Store in memory instead of file
        parquet_buffer = BytesIO()
        weather_parquet_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Uploaded to Azure Blob Storage as {blob_name}")
    except Exception as e:
        logging.error(f"Failed to upload to Azure Blob Storage: {str(e)}")
        raise


# DAG Definition
with DAG(
    dag_id="extract_weather_data_and_upload_to_blob",
    description="Extract data from openWeather API and upload to Azure Blob Storage on hourly basis",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
    tags=["weather", "azure", "keyvault"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather_data,
        provide_context=True,
    )

    upload = PythonOperator(
        task_id="upload_to_blob",
        python_callable=upload_to_blob,
        provide_context=True,
    )

    extract >> upload
