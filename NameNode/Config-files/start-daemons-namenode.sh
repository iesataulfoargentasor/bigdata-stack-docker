#!/bin/bash

HADOOP_HOME=/opt/bd/hadoop
SERVICE=${HADOOP_HOME}/bin/hdfs
DAEMON=namenode

# Si el volumen ya tiene metadata, format termina con error y se conserva.
${SERVICE} namenode -format -nonInteractive >/tmp/namenode-format.log 2>&1 || true

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

# Los directorios de HDFS solo tocan metadata: no hace falta que el DataNode
# esté registrado. Se reintenta porque el NameNode puede seguir en safemode.
ok=0
for _ in $(seq 1 30); do
  if ${SERVICE} dfs -mkdir -p /user/hdadmin /tmp/hadoop-yarn/staging && \
     ${SERVICE} dfs -chmod 1777 /tmp /tmp/hadoop-yarn /tmp/hadoop-yarn/staging; then
    ok=1
    break
  fi
  sleep 2
done

if [ "$ok" -ne 1 ]; then
  echo "No se pudieron crear los directorios iniciales de HDFS"
  exit 1
fi

touch /tmp/namenode-ready

while true
do
  sleep 10
  if ! ps aux | grep "[j]ava" | grep -q "${DAEMON}"
  then
    echo "El demonio ${DAEMON} ha fallado"
    exit 1
  fi
done
