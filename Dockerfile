# Extended Apache Airflow image.
# Add custom Python dependencies via requirements.txt and they get baked into the image.
# Docs: https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.3.1

# Install extra Python dependencies as the airflow user (never as root).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
