# Airflow 2.10 con dlt y dbt para orquestar el mismo ELT que se prueba en el nodo lab.
# dlt y dbt van en su propio entorno (/opt/elt-venv): sus dependencias chocan
# con las de Airflow si se instalan juntas.
#
#   docker build -t profesorbigdata/airflow-image-profesor:v2.0 .
FROM apache/airflow:2.10.5-python3.12
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

USER root
RUN python -m venv /opt/elt-venv && \
    chown -R airflow:0 /opt/elt-venv

USER airflow
COPY requirements-elt.txt /tmp/requirements-elt.txt
# La imagen define PIP_USER=true, que pip rechaza dentro de un venv.
RUN PIP_USER=false /opt/elt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-elt.txt
