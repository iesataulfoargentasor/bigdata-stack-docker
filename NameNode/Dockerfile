FROM profesorbigdata/hadoop-base-image:v2.0
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

USER root

ENV HADOOP_VERSION=3.3.6
ENV LOG_TAG="[NameNode Hadoop_${HADOOP_VERSION}]:"
ENV BASE_DIR=/opt/bd
ENV HADOOP_HOME=${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV DATA_DIR=/var/data/hadoop/hdfs
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=${PATH}:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

RUN echo "$LOG_TAG Crea directorios para los datos de HDFS del NameNode" && \
    mkdir -p ${DATA_DIR}/nn ${HADOOP_HOME}/logs && \
    chown -R hdadmin:hadoop ${DATA_DIR} ${HADOOP_HOME}/logs

COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/hdfs-site-namenode.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY Config-files/start-daemons-namenode.sh ${BASE_DIR}/start-daemons.sh

RUN chmod +x ${BASE_DIR}/start-daemons.sh && \
    chown hdadmin:hadoop ${BASE_DIR}/start-daemons.sh \
      ${HADOOP_CONF_DIR}/core-site.xml \
      ${HADOOP_CONF_DIR}/hdfs-site.xml

EXPOSE 9000 9870

USER hdadmin
WORKDIR ${HADOOP_HOME}
CMD ["/opt/bd/start-daemons.sh"]
