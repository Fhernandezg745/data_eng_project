FROM apache/airflow:2.7.0

COPY requirements.txt /opt/airflow
RUN pip install -r /opt/airflow/requirements.txt

# Copy your DAGs
COPY --chown=airflow:root . /opt/airflow

RUN airflow db init

# Start the scheduler and webserver
ENTRYPOINT airflow scheduler & airflow webserver
