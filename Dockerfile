FROM apache/airflow:3.0.2-python3.12

USER root
RUN apt-get -y update && apt-get -y install git gcc g++
USER airflow

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

COPY --chown=airflow:root README.md uv.lock pyproject.toml ./config/webserver_config.py /opt/airflow/
COPY --chown=airflow:root ./ils_middleware /opt/airflow/ils_middleware

RUN uv build
RUN uv pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" dist/*.whl
