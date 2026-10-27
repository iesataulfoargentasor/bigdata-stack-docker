#!/bin/bash
# Este directorio sólo contiene los PID de los demonios de este contenedor.
export HADOOP_PID_DIR=/tmp/hadoop-run
export YARN_PID_DIR=/tmp/hadoop-run
mkdir -p /tmp/hadoop-run
rm -f /tmp/hadoop-run/*.pid /tmp/resourcemanager-ready /tmp/dnnm-ready

HADOOP_HOME=/opt/bd/hadoop
SERVICE=${HADOOP_HOME}/bin/yarn
DAEMON=resourcemanager

${SERVICE} --daemon start ${DAEMON}
status=$?
if [ $status -ne 0 ]; then
  echo "No pudo inicializar el servicio ${DAEMON}: $status"
  exit $status
fi

while ! ps aux | grep "[j]ava" | grep -q "${DAEMON}"
do
  sleep 1
done

touch /tmp/resourcemanager-ready

while true
do
  sleep 10
  if ! ps aux | grep "[j]ava" | grep -q "${DAEMON}"
  then
    echo "El demonio ${DAEMON} ha fallado"
    exit 1
  fi
done
