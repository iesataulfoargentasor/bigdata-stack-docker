#!/bin/bash
set -e

# El volumen de los cuadernos se monta desde el anfitrión. Hay que poder
# escribir en él con el usuario hdadmin.
chmod 777 /media/notebooks 2>/dev/null || true

wait_for() {
  local host=$1
  local port=$2
  echo "Esperando a ${host}:${port}"
  for _ in $(seq 1 60); do
    if bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "Tiempo de espera agotado para ${host}:${port}"
  return 1
}

wait_for namenode 9000
wait_for resourcemanager 8032
wait_for dnnm 9866

export HOME=/opt/bd
export USER=hdadmin
export LOGNAME=hdadmin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/bd/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export PATH="${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

# su restablece PATH al valor de /etc/login.defs. Si no se vuelve a exportar
# dentro del comando, el kernel de Jupyter no encuentra hadoop, hdfs ni mapred.
exec su -m -s /bin/bash hdadmin -c \
  "export HOME='${HOME}' USER='${USER}' LOGNAME='${LOGNAME}' \
          JAVA_HOME='${JAVA_HOME}' HADOOP_HOME='${HADOOP_HOME}' \
          HADOOP_CONF_DIR='${HADOOP_CONF_DIR}' PATH='${PATH}'; \
   exec /opt/jupyter_env/bin/jupyter notebook --ip=0.0.0.0 --port=8889 --notebook-dir=/media/notebooks --no-browser"
