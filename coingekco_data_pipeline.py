from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
import time

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'coingecko_to_bigquery',
    default_args=default_args,
    description='Fetch CoinGecko data --> Store Raw --> Clean --> Store Clean',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    start_date=datetime(2025, 10, 13),
    catchup=False,
)

# BigQuery project and dataset info
project_id = "worldbank-data-474912"
raw_dataset = "crypto_raw"
clean_dataset = "crypto_clean"
table_name = "coingecko_data"

# Initialize BigQuery client
bq_client = bigquery.Client(project=project_id)

# Ensure datasets exist
for ds in [raw_dataset, clean_dataset]:
    try:
        bq_client.get_dataset(ds)
        print(f"Dataset exists: {project_id}.{ds}")
    except Exception:
        bq_client.create_dataset(ds)
        print(f"Dataset created: {project_id}.{ds}")


def fetch_coingecko_data(pages=8, per_page=250, delay=4, **context):
    """
    Fetch paginated crypto market data from CoinGecko API.
    """
    all_data = []
    for page in range(1, pages + 1):
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": False
        }

        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 429:
            print(" Rate limit hit waiting 5 seconds...")
            time.sleep(5)
            continue

        response.raise_for_status()
        data = response.json()
        all_data.extend(data)
        print(f"Page {page} fetched with {len(data)} rows.")

        time.sleep(delay)  # Wait 4 seconds between pages

    df = pd.DataFrame(all_data)
    df["fetch_time"] = datetime.utcnow().isoformat()
    print(f"Total rows fetched: {len(df)}")

    return df.to_json(orient="records")


def load_raw_to_bigquery(**context):
    """
    Load raw CoinGecko data into BigQuery (crypto_raw dataset).
    """
    json_data = context['ti'].xcom_pull(task_ids='fetch_data')
    df = pd.read_json(json_data)

    if 'fetch_time' in df.columns:
        df['fetch_time'] = df['fetch_time'].astype(str)

    table_ref = f"{project_id}.{raw_dataset}.{table_name}"

    job = bq_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Loaded {len(df)} rows into {table_ref}")


def clean_data(**context):
    """
    Read from crypto_raw, clean the data, and return JSON for cleaned load.
    """
    query = f"""
        SELECT *
        FROM `{project_id}.{raw_dataset}.{table_name}`
        WHERE fetch_time = (
            SELECT MAX(fetch_time)
            FROM `{project_id}.{raw_dataset}.{table_name}`
        )
    """
    df = bq_client.query(query).to_dataframe()
    print(f"Cleaning {len(df)} raw rows...")

    # Drop duplicates and rows missing key fields
    df = df.drop_duplicates(subset=["id"])
    df = df.dropna(subset=["id", "symbol", "name"])

    # Keep relevant columns
    keep_cols = [
        "id", "symbol", "name", "current_price", "market_cap",
        "total_volume", "high_24h", "low_24h", "price_change_percentage_24h",
        "fetch_time"
    ]
    df = df[[col for col in keep_cols if col in df.columns]]

    print(f"Cleaned data: {len(df)} rows remain.")
    return df.to_json(orient="records")


def load_clean_to_bigquery(**context):
    """
    Load cleaned data into BigQuery (crypto_clean dataset).
    """
    json_data = context['ti'].xcom_pull(task_ids='clean_data')
    df = pd.read_json(json_data)
    
    # Convert fetch_time to string to avoid pyarrow errors
    if 'fetch_time' in df.columns:
        df['fetch_time'] = df['fetch_time'].astype(str)

    table_ref = f"{project_id}.{clean_dataset}.{table_name}"

    job = bq_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f" Cleaned data loaded into {table_ref} ({len(df)} rows).")

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_coingecko_data,
    provide_context=True,
    dag=dag,
)

load_raw_task = PythonOperator(
    task_id='load_raw_to_bigquery',
    python_callable=load_raw_to_bigquery,
    provide_context=True,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

load_clean_task = PythonOperator(
    task_id='load_clean_to_bigquery',
    python_callable=load_clean_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Task order (ETL flow)
fetch_task >> load_raw_task >> clean_task >> load_clean_task
