from datetime import datetime
import json
import requests
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pathlib import Path


BUCKET_NAME = "test-for-etl-proj"


def extract_and_upload():
    # 1. Call a public API (no auth)
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    payload = {
        "extracted_at": datetime.utcnow().isoformat(),
        "data": data
    }

    # 2. Get AWS credentials from Airflow connection
    from botocore.config import Config

    aws_hook = AwsBaseHook(aws_conn_id="aws_default", client_type="s3")
    session = aws_hook.get_session()

    s3_client = session.client(
        "s3",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "virtual"})
    )


    # 3. Upload to S3
    s3_key = f"raw/api/crypto_prices_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )


with DAG(
    dag_id="api_to_s3_crypto",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",   # every 2 minutes
    catchup=False,
    tags=["etl", "api", "s3"],
    max_active_runs=1,                 # prevents overlap
) as dag:


    extract_upload_task = PythonOperator(
        task_id="extract_api_and_upload_to_s3",
        python_callable=extract_and_upload
    )
    

    load_to_snowflake = SnowflakeOperator(
    task_id="load_to_snowflake",
    snowflake_conn_id="snowflake_default",
    sql="sql/Crypto-etl.sql",  
    split_statements=False
)


    extract_upload_task >> load_to_snowflake
