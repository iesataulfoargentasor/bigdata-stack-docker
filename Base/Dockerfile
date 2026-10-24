# Imagen base del clúster. No se ejecuta como servicio.
# La construye docker compose antes que el resto (servicio hadoop-base).
# También se puede construir sola:
#   docker build -t profesorbigdata/hadoop-base-image:v2.0 .
FROM ubuntu:22.04
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

USER root

# Hadoop 3.3.6 funciona con Java 8. Ubuntu 22.04 todavía publica openjdk-8.
# El tarball se baja del archivo de Apache: dlcdn solo conserva la última versión.
ENV HADOOP_VERSION=3.3.6
ENV BASE_DIR=/opt/bd
ENV LOG_TAG="[BASE Hadoop_${HADOOP_VERSION}]:"
ENV REPOSITORY=https://archive.apache.org/dist/hadoop/common
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV LANG=es_ES.UTF-8
ENV LC_ALL=es_ES.UTF-8

RUN echo "$LOG_TAG Actualizando e instalando paquetes básicos" && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-8-jdk \
        libbcprov-java \
        python3 \
        python3-pip \
        curl \
        ca-certificates \
        locales \
        iputils-ping \
        procps \
        nano && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN echo "$LOG_TAG Creando locales" && \
    sed -i '/es_ES.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen es_ES.UTF-8 && \
    update-locale LANG=es_ES.UTF-8

RUN mkdir -p ${BASE_DIR}
WORKDIR ${BASE_DIR}

RUN echo "$LOG_TAG Descargando Hadoop" && \
    curl -fL --retry 3 --retry-delay 2 -o hadoop-${HADOOP_VERSION}.tar.gz \
        "${REPOSITORY}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    ln -s hadoop-${HADOOP_VERSION} hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh

# hdadmin ejecuta los demonios y el cliente. El directorio temporal tiene que
# existir y ser suyo: core-site.xml apunta a /var/tmp/hadoop-${user.name}.
RUN groupadd -r hadoop && \
    useradd -r -g hadoop -d ${BASE_DIR} -s /bin/bash hdadmin && \
    mkdir -p /var/tmp/hadoop-hdadmin ${HADOOP_HOME}/logs && \
    chown hdadmin:hadoop /var/tmp/hadoop-hdadmin ${HADOOP_HOME}/logs ${HADOOP_CONF_DIR}/hadoop-env.sh
