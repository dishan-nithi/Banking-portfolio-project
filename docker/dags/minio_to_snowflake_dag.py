import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

MINIO_BUCKET = "raw"
LOCAL_DOWNLOAD_DIR = "/opt/airflow/tmp_downloads"
TABLES = ["customers", "accounts", "transactions"]

default_args = {
    "owner": "banking-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="minio_to_snowflake",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["banking", "ingestion"],
)
def minio_to_snowflake_dag():

    @task
    def list_and_download_files(table: str) -> list[str]:
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=f"{table}/")
        if not keys:
            print(f"No files found under {table}/")
            return []

        local_dir = os.path.join(LOCAL_DOWNLOAD_DIR, table)
        os.makedirs(local_dir, exist_ok=True)

        s3_client = s3_hook.get_conn()
        local_paths = []
        for key in keys:
            filename = os.path.basename(key)
            local_path = os.path.join(local_dir, filename)
            s3_client.download_file(MINIO_BUCKET, key, local_path)
            local_paths.append(local_path)

        print(f"Downloaded {len(local_paths)} files for {table}")
        return local_paths

    @task
    def load_to_snowflake(table: str, local_paths: list[str]):
        if not local_paths:
            print(f"Nothing to load for {table}")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            for path in local_paths:
                put_path = path.replace("\\", "/")
                cur.execute(
                    f"PUT file://{put_path} @banking_internal_stage/{table}/ "
                    f"OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
                )

            copy_result = cur.execute(f"""
                COPY INTO raw.{table}
                FROM @banking_internal_stage/{table}/
                FILE_FORMAT = (FORMAT_NAME = parquet_format)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = ABORT_STATEMENT
                PURGE = TRUE
            """).fetchall()
            print(f"COPY INTO raw.{table} result: {copy_result}")
        finally:
            cur.close()
            conn.close()

    for table in TABLES:
        files = list_and_download_files.override(task_id=f"list_and_download_{table}")(table)
        load_to_snowflake.override(task_id=f"load_{table}_to_snowflake")(table, files)


minio_to_snowflake_dag()