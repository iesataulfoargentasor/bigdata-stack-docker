# Cliente del clúster. No arranca demonios: desde aquí se usa HDFS, se envían
# trabajos a YARN y se abre Jupyter.
FROM profesorbigdata/hadoop-base-image:v2.0
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

USER root

ENV BASE_DIR=/opt/bd
ENV HADOOP_HOME=${BASE_DIR}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HOME=${BASE_DIR}
ENV PATH=${PATH}:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

RUN apt-get update && \
    apt-get install -y -o Acquire::Retries=5 --no-install-recommends python3-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# El proceso de Jupyter corre como hdadmin. La configuración tiene que estar
# en su home (/opt/bd), no en /root.
RUN python3 -m venv /opt/jupyter_env && \
    /opt/jupyter_env/bin/pip install --upgrade pip && \
    /opt/jupyter_env/bin/pip install 'notebook>=7.2,<8' && \
    mkdir -p ${HOME}/.jupyter ${HOME}/.local /media/notebooks && \
    printf "%s\n" \
      "c.ServerApp.token = ''" \
      "c.ServerApp.password = ''" \
      "c.ServerApp.ip = '0.0.0.0'" \
      "c.ServerApp.port = 8889" \
      "c.ServerApp.root_dir = '/media/notebooks'" \
      "c.ServerApp.open_browser = False" \
      "c.IdentityProvider.token = ''" \
      > ${HOME}/.jupyter/jupyter_server_config.py && \
    cp ${HOME}/.jupyter/jupyter_server_config.py ${HOME}/.jupyter/jupyter_notebook_config.py && \
    chown -R hdadmin:hadoop /opt/jupyter_env ${HOME}/.jupyter ${HOME}/.local /media/notebooks

COPY Config-files/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY Config-files/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY Config-files/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml
COPY Config-files/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY Config-files/start-client.sh ${BASE_DIR}/start-client.sh

# /usr/local/bin ya está en el PATH por defecto. Así los comandos del cuaderno
# funcionan aunque su haya restablecido PATH.
RUN ln -sfn ${HADOOP_HOME}/bin/hadoop /usr/local/bin/hadoop && \
    ln -sfn ${HADOOP_HOME}/bin/hdfs /usr/local/bin/hdfs && \
    ln -sfn ${HADOOP_HOME}/bin/yarn /usr/local/bin/yarn && \
    ln -sfn ${HADOOP_HOME}/bin/mapred /usr/local/bin/mapred && \
    chmod +x ${BASE_DIR}/start-client.sh && \
    chown hdadmin:hadoop ${BASE_DIR}/start-client.sh \
      ${HADOOP_CONF_DIR}/core-site.xml \
      ${HADOOP_CONF_DIR}/hdfs-site.xml \
      ${HADOOP_CONF_DIR}/yarn-site.xml \
      ${HADOOP_CONF_DIR}/mapred-site.xml

EXPOSE 8889
WORKDIR /media/notebooks
CMD ["/opt/bd/start-client.sh"]
