#!/bin/bash

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
