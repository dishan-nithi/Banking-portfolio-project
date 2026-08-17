FROM apache/airflow:3.3.1
COPY dbt_core_experimental_parser-2.0.0b1-py3-none-manylinux_2_28_x86_64.whl /tmp/
RUN pip install --no-cache-dir --retries 5 boto3 pandas pyarrow python-dotenv
RUN pip install --no-cache-dir --retries 5 apache-airflow-providers-amazon apache-airflow-providers-snowflake
RUN pip install --no-cache-dir --retries 5 snowflake-connector-python
RUN pip install --no-cache-dir /tmp/dbt_core_experimental_parser-2.0.0b1-py3-none-manylinux_2_28_x86_64.whl
RUN pip install --no-cache-dir --retries 5 dbt-core dbt-snowflake