#!/bin/bash
# Este directorio sólo contiene los PID de los demonios de este contenedor.
export HADOOP_PID_DIR=/tmp/hadoop-run
export YARN_PID_DIR=/tmp/hadoop-run
mkdir -p /tmp/hadoop-run
rm -f /tmp/hadoop-run/*.pid /tmp/resourcemanager-ready /tmp/dnnm-ready

HADOOP_HOME=/opt/bd/hadoop

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

wait_for namenode 9000 || exit 1
wait_for resourcemanager 8032 || exit 1

${HADOOP_HOME}/bin/hdfs --daemon start datanode
status=$?
if [ $status -ne 0 ]; then
  echo "No pudo inicializar el servicio datanode: $status"
  exit $status
fi

${HADOOP_HOME}/bin/yarn --daemon start nodemanager
status=$?
if [ $status -ne 0 ]; then
  echo "No pudo inicializar el servicio nodemanager: $status"
  exit $status
fi

while ! ps aux | grep "[j]ava" | grep -q "datanode" || \
      ! ps aux | grep "[j]ava" | grep -q "nodemanager"
do
  sleep 1
done

touch /tmp/dnnm-ready

while true
do
  sleep 10
  if ! ps aux | grep "[j]ava" | grep -q "datanode"
  then
    echo "El demonio datanode ha fallado"
    exit 1
  fi
  if ! ps aux | grep "[j]ava" | grep -q "nodemanager"
  then
    echo "El demonio nodemanager ha fallado"
    exit 1
  fi
done
