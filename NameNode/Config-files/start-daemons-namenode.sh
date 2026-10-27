#!/bin/bash
set -euo pipefail
HADOOP_HOME=/opt/bd/hadoop
# Formatear sólo un volumen nuevo; nunca ocultar un error de formato.
if [ ! -f /var/data/hadoop/hdfs/nn/current/VERSION ]; then
  "${HADOOP_HOME}/bin/hdfs" namenode -format -nonInteractive
fi
# El DataNode debe poder registrarse antes de que HDFS salga del modo seguro.
# Los directorios se preparan después, en hdfs-init.
exec "${HADOOP_HOME}/bin/hdfs" namenode
