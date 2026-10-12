FROM apache/airflow:3.2.0-python3.12

USER root
RUN apt-get -y update && apt-get -y install git gcc g++
USER airflow

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

COPY --chown=airflow:root README.md uv.lock pyproject.toml /opt/airflow/
COPY --chown=airflow:root ./ils_middleware /opt/airflow/ils_middleware
COPY --chown=airflow:root ./plugins /opt/airflow/plugins

RUN uv build
RUN uv pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" dist/*.whl

# sqlalchemy-utils (<=0.42.1, required by flask-appbuilder) uses names that SQLAlchemy 2.1 made private.
# Remove once an upstream sqlalchemy-utils release supports SQLAlchemy 2.1.
RUN sed -i \
      -e 's/attributes\.ScalarAttributeImpl/attributes._ScalarAttributeImpl/' \
      -e 's/attributes\.register_attribute/attributes._register_attribute/' \
      "$(python -c 'import importlib.util; print(importlib.util.find_spec("sqlalchemy_utils").submodule_search_locations[0])')/generic.py"
