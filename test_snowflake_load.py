import os
import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "banking_wh")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "banking")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "raw")

LOCAL_DIR = "snowflake_test_downloads"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def find_one_customer_file():
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="customers/")
    contents = response.get("Contents", [])
    if not contents:
        raise RuntimeError("No files found under customers/ in MinIO yet")
    return contents[0]["Key"]


def download_file(key):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    local_path = os.path.abspath(os.path.join(LOCAL_DIR, os.path.basename(key)))
    s3.download_file(MINIO_BUCKET, key, local_path)
    print(f"Downloaded {key} to {local_path}")
    return local_path


def main():
    key = find_one_customer_file()
    local_path = download_file(key)
    put_path = local_path.replace("\\", "/")

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{put_path} @banking_internal_stage OVERWRITE = TRUE")
        for row in cur.fetchall():
            print(row)

        staged_filename = os.path.basename(local_path)
        cur.execute(f"""
            COPY INTO raw.customers
            FROM @banking_internal_stage/{staged_filename}
            FILE_FORMAT = (FORMAT_NAME = parquet_format)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        for row in cur.fetchall():
            print(row)

        cur.execute("SELECT * FROM raw.customers")
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()