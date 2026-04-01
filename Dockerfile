FROM apache/airflow:2.9.3

# Copy requirements into the image
COPY requirements.txt /requirements.txt

# Install Python packages as the airflow user (required by Airflow image)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
