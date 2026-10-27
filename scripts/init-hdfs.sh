#!/bin/bash
set -euo pipefail
# Espera acotada: no forzar la salida del modo seguro ni borrar datos.
timeout 300 hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /user/hdadmin /tmp/hadoop-yarn/staging /user/hive/warehouse /tmp/hive
hdfs dfs -chmod 1777 /tmp /tmp/hadoop-yarn /tmp/hadoop-yarn/staging
hdfs dfs -chmod -R 777 /user/hive /tmp/hive
hdfs dfs -ls /user /tmp
