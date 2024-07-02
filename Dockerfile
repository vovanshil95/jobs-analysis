FROM apache/airflow
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
