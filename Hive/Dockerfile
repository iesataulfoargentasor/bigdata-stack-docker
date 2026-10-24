# Hive 4.0.1 para el metastore y HiveServer2.
# La imagen oficial trae Hadoop 3.3.6, como el clúster, pero le faltan dos cosas:
#   - el driver JDBC de MySQL, para guardar el catálogo en mysql-metastore
#   - hadoop-aws en el classpath, para que el catálogo entienda rutas s3a:// (MinIO)
#
#   docker build -t profesorbigdata/hive-image-profesor:v2.0 .
FROM apache/hive:4.0.1
LABEL maintainer="Profesor <jose.martin@educantabria.es>"

ENV MYSQL_CONNECTOR_VERSION=8.4.0

USER root

# La imagen no trae curl ni wget: ADD descarga el jar directamente.
ADD --chmod=644 \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar \
    /opt/hive/lib/

# hadoop-aws y su SDK vienen en tools/lib, que no está en el classpath de Hive.
RUN for jar in hadoop-aws-3.3.6.jar aws-java-sdk-bundle-1.12.367.jar; do \
      test -f /opt/hadoop/share/hadoop/tools/lib/$jar && \
      ln -s /opt/hadoop/share/hadoop/tools/lib/$jar /opt/hive/lib/$jar || exit 1; \
    done

USER hive
