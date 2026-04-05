FROM apache/airflow:2.10.0-python3.12

RUN pip install --no-cache-dir \
    "boto3>=1.34" \
    "requests>=2.31" \
    "python-dotenv>=1.0" \
    "websockets>=16.0"