# Airflow 3 + dependencias del proyecto (mismas versiones que requirements.txt).
FROM apache/airflow:3.2.2-python3.10

COPY requirements.txt /tmp/requirements.txt
# Se fija apache-airflow para que pip no lo cambie al resolver nuestras dependencias.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
