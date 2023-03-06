FROM apache/airflow:2.5.1-python3.10
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install 'apache-airflow-providers-redis'
RUN pip install --no-cache-dir --user -r /requirements.txt
