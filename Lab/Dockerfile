# Nodo de trabajo del ELT: DuckDB + dlt + dbt, con JupyterLab y el CLI de DuckDB.
# También tiene boto3 y el CLI de AWS para hablar con MinIO.
#
#   docker build -t profesorbigdata/lab-image-profesor:v2.0 .
FROM python:3.12-slim-bookworm
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

ENV DUCKDB_VERSION=1.3.2
ENV LANG=C.UTF-8
# El contenedor corre con el uid de Airflow (AIRFLOW_UID) para que los dos
# puedan escribir retail.duckdb. Ese uid no existe en la imagen: HOME tiene
# que ser un directorio fijo y escribible por cualquiera.
ENV HOME=/home/lab

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl unzip ca-certificates default-mysql-client git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN arch=$(dpkg --print-architecture) && \
    curl -fsSL -o /tmp/duckdb.zip \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${arch}.zip" && \
    unzip /tmp/duckdb.zip -d /usr/local/bin && \
    rm /tmp/duckdb.zip

# Las extensiones se instalan ahora para no depender de Internet en clase.
RUN mkdir -p ${HOME} /workspace && \
    duckdb -c "INSTALL httpfs; INSTALL mysql;" && \
    python -c "import duckdb; c = duckdb.connect(); c.install_extension('httpfs'); c.install_extension('mysql')" && \
    chmod -R a+rwX ${HOME} /workspace
WORKDIR /workspace

EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=", "--ServerApp.password=", "--ServerApp.root_dir=/workspace"]
