# Airflow 3 + dependencias del proyecto (mismas versiones que requirements.txt).
FROM apache/airflow:3.2.2-python3.10

# LightGBM necesita OpenMP (libgomp) en la imagen base.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /tmp/requirements.txt
# Se fija apache-airflow para que pip no lo cambie al resolver nuestras dependencias.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
