# Stack de Big Data en Docker

Ecosistema completo de ingeniería de datos para clase, levantado con Docker Compose:
**HDFS + YARN**, **MinIO**, **Hive Metastore + HiveServer2**, **Trino**, un nodo de trabajo
con **DuckDB + dlt + dbt** y **Airflow** para orquestar.

Parte del clúster Hadoop del profesor (imágenes `profesorbigdata/*:v2.0`) y añade las herramientas
del stack docente de ingeniería de datos.

📘 **Guía de despliegue paso a paso:** https://iesataulfoargentasor.github.io/bigdata-stack-docker/

> **Stack docente, no de producción.** HDFS con permisos abiertos, credenciales triviales
> (`minioadmin`, `admin`, `retail`...) y Jupyter sin token. Pensado para que funcione a la
> primera en el aula, nunca para exponerlo a Internet.

---

## Índice

1. [Arquitectura](#arquitectura)
2. [Profiles: qué se arranca en cada sesión](#profiles-qué-se-arranca-en-cada-sesión)
3. [Requisitos](#requisitos)
4. [Despliegue rápido](#despliegue-rápido)
5. [Servicios, puertos y credenciales](#servicios-puertos-y-credenciales)
6. [Los contenedores](#los-contenedores)
7. [Recorrido 1: consultar el lago (`lake`)](#recorrido-1-consultar-el-lago-lake)
8. [Recorrido 2: ELT con dlt, dbt y Airflow (`elt`)](#recorrido-2-elt-con-dlt-dbt-y-airflow-elt)
9. [Operación diaria](#operación-diaria)
10. [Acceso desde el aula (Tailscale)](#acceso-desde-el-aula)
11. [Estructura del repositorio](#estructura-del-repositorio)
12. [Decisiones técnicas](#decisiones-técnicas)
13. [Solución de problemas](#solución-de-problemas)
14. [Otros compose del repositorio](#otros-compose-del-repositorio)

---

## Arquitectura

El stack se organiza en capas: **orquestación** arriba, **motores** que ejecutan el trabajo,
**catálogo y origen** de datos, **almacenamiento** (HDFS y MinIO conviven a propósito) y el
**cómputo** de YARN. Aparte, varios contenedores `*-init` preparan el entorno y terminan.

```mermaid
flowchart TB
    subgraph ORQ["🗓️ Orquestación"]
        AF["Airflow<br/>webserver :8082 · scheduler"]
        PG[("postgres-airflow")]
    end

    subgraph MOT["⚙️ Motores"]
        TR["Trino :8080"]
        HS2["HiveServer2 :10000<br/>(Tez local)"]
        LAB["lab :8888<br/>JupyterLab · DuckDB · dlt · dbt"]
    end

    subgraph CAT["📚 Catálogo y origen"]
        HMS["Hive Metastore :9083"]
        MYM[("mysql-metastore<br/>metadatos")]
        MYR[("mysql-retail<br/>retail_db")]
    end

    subgraph ALM["💾 Almacenamiento"]
        subgraph HDFS["HDFS"]
            NN["namenode :9870"]
            DN["dnnm (DataNode) :9864"]
        end
        MINIO["MinIO :9000 / :9001<br/>raw-data · processed · warehouse"]
    end

    subgraph COMP["🧮 Cómputo"]
        RM["resourcemanager :8088"]
        NM["dnnm (NodeManager) :8042"]
    end

    AF --- PG
    AF -. "dlt → dbt" .-> LAB
    TR -- metadatos --> HMS
    HS2 -- metadatos --> HMS
    HMS --- MYM
    TR == datos ==> DN
    TR == datos ==> MINIO
    HS2 == datos ==> DN
    HS2 == datos ==> MINIO
    TR -- federación --> MYR
    LAB -- "EL (dlt)" --> MYR
    LAB -- "S3 (boto3/aws)" --> MINIO
    NN --- DN
    RM --- NM
```

Hay tres tipos de tráfico y conviene distinguirlos:

| Tráfico | Ejemplo | Idea clave |
|---|---|---|
| **Metadatos** | Trino pregunta al metastore qué columnas tiene una tabla y en qué ruta está | El catálogo **no guarda datos**, sólo apunta a ellos |
| **Datos** | Conocida la ruta, el motor lee los ficheros del DataNode o de MinIO | El motor va **directo** al almacenamiento |
| **Orquestación** | Airflow lanza dlt y después dbt | El orquestador **no mueve datos**, coordina |

Esta separación entre *dónde está el dato* y *el dato en sí* es la idea central de un lago de
datos (la misma que Athena + Glue sobre S3).

### Orden de arranque

El arranque está encadenado con `healthcheck` y `depends_on`: las dependencias esperan sus puertos o la finalización de la inicialización.
El NameNode abre su RPC antes de recibir al DataNode; `hdfs-init` espera la salida
del modo seguro y prepara los permisos. Trino espera también a que retail tenga datos.

```mermaid
flowchart LR
    subgraph lake["profile lake"]
        direction LR
        nn[namenode] --> dnnm --> hi[hdfs-init] --> hms[hive-metastore] --> hs2[hiveserver2]
        rm[resourcemanager] --> dnnm
        mym[mysql-metastore] --> hms
        hms --> tr[trino]
        mi[minio] --> mii[minio-init] --> tr
        myr[mysql-retail] --> ri[retail-init] --> tr
    end
```

```mermaid
flowchart LR
    subgraph elt["profile elt"]
        direction LR
        myr[mysql-retail] --> ri[retail-init] --> lab
        mi[minio] --> mii[minio-init] --> lab
        pg[postgres-airflow] --> ai[airflow-init] --> ws[airflow-webserver]
        ai --> sc[airflow-scheduler]
        ri --> sc
    end
```

Los contenedores `hdfs-init`, `minio-init`, `retail-init` y `airflow-init` hacen su trabajo y
terminan: verlos como **`Exited (0)`** es lo correcto.

---

## Profiles: qué se arranca en cada sesión

**Nada arranca por defecto.** Todos los servicios están detrás de *profiles* de Compose, así
sólo se enciende lo que se necesita y se ahorra memoria. Esto se refiere a `docker-compose.stack.yml`. Sin `-f`, Compose selecciona
`docker-compose.yml` y sí levanta Hadoop (salvo que hayas definido `COMPOSE_FILE`).

### Profiles de componente

| Profile | Servicios | Para qué |
|---|---|---|
| `hadoop` | namenode, resourcemanager, dnnm, client, hdfs-init | HDFS + YARN (+ Jupyter con cliente Hadoop) |
| `minio` | minio, minio-init | Object storage (el lago "moderno") |
| `hive` | HDFS + YARN + MinIO + inicialización + MySQL metastore + Hive | Hive con sus almacenamientos |
| `trino` | Hive con dependencias + retail e inicialización + Trino | Todos los catálogos disponibles |
| `lab` | mysql-retail, retail-init, lab | DuckDB + dlt + dbt (y `retail_db`) |
| `airflow` | mysql-retail, retail-init, postgres-airflow, airflow-init, airflow-webserver, airflow-scheduler | Orquestación del ELT |

### Profiles paraguas

| Profile | Equivale a | Para qué |
|---|---|---|
| `lake` | hadoop + minio + hive + trino + mysql-retail | Consultar el lago (sin `client` ni `lab`) |
| `elt` | minio + lab + airflow | Ingesta, dbt y orquestación |
| `all` | todo | Levantarlo entero (necesita mucha memoria) |

```mermaid
flowchart LR
    lake([lake]):::paraguas
    elt([elt]):::paraguas
    all([all]):::paraguas

    hadoop[hadoop]
    minio[minio]
    hive[hive]
    trino[trino]
    lab[lab]
    airflow[airflow]
    retail[(mysql-retail<br/>retail-init)]

    lake --> hadoop & minio & hive & trino & retail
    elt --> minio & lab & airflow
    all --> hadoop & minio & hive & trino & lab & airflow
    lab --> retail
    airflow --> retail

    classDef paraguas fill:#1f6feb,color:#fff,stroke:#1f6feb
```

> `mysql-retail` viaja también con `lake` porque Trino **federa** contra `retail_db` con su
> conector MySQL.

Los profiles se pueden combinar. Por ejemplo, para comparar Trino y DuckDB sobre el mismo lago:

```bash
docker compose -f docker-compose.stack.yml --profile lake --profile lab up -d
```

---

## Requisitos

| | Mínimo | Recomendado |
|---|---|---|
| Docker | Docker Desktop / Engine con **Compose v2.20+** | Última versión |
| Memoria para Docker | 8 GB (un escenario: `lake` **o** `elt`) | 12 GB o más para `all` |
| Disco libre | ~15 GB (imágenes + volúmenes) | 25 GB |
| CPU | 4 núcleos | 8 núcleos |
| Arquitectura | amd64 | amd64 (en Apple Silicon Hadoop y Hive corren emulados) |

> **Windows:** conviene que el disco virtual de Docker esté en una unidad con espacio
> (Docker Desktop → *Settings → Resources → Advanced → Disk image location*). Si el equipo se
> queda sin memoria justo después de construir imágenes, revisa el proceso `docker-scout` en el
> Administrador de tareas: puede ocupar varios GB.

---

## Despliegue rápido

### Preparar Windows 11 o Ubuntu 22.04

Instala Git y Docker. En Windows 11, abre Docker Desktop con el motor WSL 2 y contenedores Linux. En Ubuntu 22.04, utiliza Docker Engine con el complemento Compose v2 o Docker Desktop. Usa siempre el mismo motor/contexto para acceder a tus volúmenes.

Comprueba en PowerShell (Windows) o Terminal (Ubuntu):

```text
docker version
docker compose version
```

docker version debe mostrar Client y Server. Si sólo aparece Client o un error 500, resuelve primero el arranque de Docker. Compose debe ser 2.20 o posterior.

### Descargar y preparar el proyecto

```text
git clone https://github.com/iesataulfoargentasor/bigdata-stack-docker.git
cd bigdata-stack-docker
```

Windows 11 — PowerShell:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Ubuntu 22.04 — Terminal:

```bash
if [ ! -f .env ]; then cp .env.example .env; fi
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
```

Esto guarda el usuario de Ubuntu en .env, para que Lab y Airflow puedan escribir en workspace. No hace falta repetirlo en cada terminal. Ejecuta Docker con tu usuario habitual, con acceso al motor; no clones ni prepares workspace con sudo. Si ya contiene archivos de otro usuario, revisa su propietario. Una variable AIRFLOW_UID exportada en la terminal prevalece sobre .env: elimínala o haz que coincida con él.

### Elegir un escenario y arrancar

Ejecuta uno de los siguientes comandos; funcionan en ambos sistemas:

```text
# Lago: HDFS + YARN + MinIO + Hive + Trino + datos retail
docker compose -f docker-compose.stack.yml --profile lake up -d --wait --wait-timeout 600

# O bien ELT: MinIO + JupyterLab + DuckDB/dlt/dbt + Airflow
docker compose -f docker-compose.stack.yml --profile elt up -d --wait --wait-timeout 600
```

La primera descarga es grande. El tiempo de arranque depende del equipo. --wait comprueba la disponibilidad declarada de los servicios, pero no sustituye a probar los ejercicios. Los servicios de inicialización deben terminar con código 0.

```text
docker compose -f docker-compose.stack.yml --profile all ps -a
```

Los scripts Hadoop del repositorio se montan sobre las imágenes publicadas: no necesitas reconstruirlas para aplicar estas correcciones. Conserva las carpetas scripts, NameNode, ResourceManager y DataNode-NodeManager junto al Compose.

### Entre sesiones y al actualizar

```text
# Parar sin borrar datos
docker compose -f docker-compose.stack.yml --profile all stop

# Volver a iniciar el escenario elegido y comprobarlo
docker compose -f docker-compose.stack.yml --profile lake up -d --wait --wait-timeout 600

# Actualizar una clonación existente
git pull --ff-only
# Después, repite el comando up del escenario que uses.
```

Si actualizas desde una versión anterior, ejecuta primero stop, prepara .env y luego usa up para aplicar los cambios. down elimina contenedores y red, pero conserva los volúmenes. No uses down -v para solucionar errores: elimina los datos persistentes. No levantes a la vez el Hadoop mínimo y el stack completo: comparten nombres y puertos.

### Comprobaciones después de arrancar

Para lake:

```text
docker compose -f docker-compose.stack.yml exec namenode hdfs dfsadmin -report
docker compose -f docker-compose.stack.yml exec namenode hdfs dfs -ls /user /tmp
docker compose -f docker-compose.stack.yml exec trino trino --execute "SELECT count(*) FROM mysql.retail_db.orders"
```

Debe haber un DataNode activo, las carpetas preparadas y 20000 pedidos en una carga nueva. Para elt, abre http://localhost:8888, crea y guarda un cuaderno; abre Airflow en http://localhost:8082 (admin/admin) y ejecuta elt_retail. Sus tres tareas deben terminar bien. Cierra las conexiones a DuckDB del cuaderno antes de ejecutar el DAG. Sus ejecuciones se procesan de una en una; no ejecutes a la vez el ELT manual y el de Airflow.

Ante un fallo:

```text
docker compose -f docker-compose.stack.yml --profile all ps -a
docker compose -f docker-compose.stack.yml logs --tail 100 NOMBRE_DEL_SERVICIO
```

Sustituye NOMBRE_DEL_SERVICIO por el que falla. No borres volúmenes para repetir el arranque.

### Kafka

```text
docker compose -f docker-compose.kafka.yml up -d --wait --wait-timeout 600
```

Abre http://localhost:8083. Los tres brokers tienen volúmenes independientes y conservan los datos tras down seguido de up. Los clientes del mismo equipo usan localhost:29092, localhost:39092 y localhost:49092. Para clientes en otros equipos, configura KAFKA_EXTERNAL_HOST en .env con la IP o DNS accesible del servidor y vuelve a ejecutar up. Los puertos 29092, 39092 y 49092 deben ser accesibles desde esos clientes. La consola web puede usarse junto con Trino, que sigue en el puerto 8080.

Si ya utilizabas Kafka antes de esta actualización, exporta primero los mensajes que quieras conservar. El diseño anterior guardaba los datos dentro de los contenedores; los volúmenes nuevos no copian automáticamente esos mensajes. Esta actualización crea un almacenamiento persistente nuevo al recrear los brokers.

### Alcance de la validación

Puedes validar los archivos Compose, las dependencias y los puertos con: python scripts/validate-compose.py (en Ubuntu: python3 scripts/validate-compose.py). Esta validación no arranca contenedores. Antes de dar una instalación por lista para clase, realiza las comprobaciones anteriores y una prueba de escritura, lectura y recreación conservando los volúmenes. El 28/10/2026 se verificaron en Windows con Docker Desktop los escenarios lake y elt y el clúster Kafka: consultas, flujo ELT y persistencia de lake y Kafka tras recrear contenedores. Ubuntu nativo y el perfil all simultáneo no se han probado en ejecución. Consulta el informe de validación para conocer el alcance exacto.

[Informe de las pruebas reales](docs/validacion-2026-10-28.txt).

---

## Servicios, puertos y credenciales

| Servicio | Puerto | Acceso | Credenciales |
|---|---|---|---|
| `namenode` | 9870 | http://localhost:9870 | — |
| `dnnm` (DataNode) | 9864 | http://localhost:9864 | — |
| `resourcemanager` | 8088 | http://localhost:8088 | — |
| `dnnm` (NodeManager) | 8042 | http://localhost:8042 | — |
| `client` (Jupyter + Hadoop CLI) | 8889 | http://localhost:8889 | sin token |
| `minio` | 9001 / 9000 | http://localhost:9001 (consola) · API S3 en `:9000` | `minioadmin` / `minioadmin` |
| `trino` | 8080 | http://localhost:8080 | cualquier usuario, sin contraseña |
| `lab` (JupyterLab) | 8888 | http://localhost:8888 | sin token |
| `airflow-webserver` | 8082 | http://localhost:8082 | `admin` / `admin` |
| `hiveserver2` | 10000 / 10002 | JDBC `jdbc:hive2://localhost:10000/` · web http://localhost:10002 | — |
| `hive-metastore` | 9083 | Thrift | — |
| `mysql-metastore` | 3306 | MySQL (catálogo) | `hive` / `hive` (root: `root`) |
| `mysql-retail` | 3307 | MySQL (`retail_db`) | `retail` / `retail` (root: `root`) |

---

## Los contenedores

| Contenedor | Imagen | Qué hace |
|---|---|---|
| `namenode` | `profesorbigdata/hadoop-namenode-image-profesor:v2.0` | Cerebro de HDFS: árbol de directorios y ubicación de bloques. RPC en `namenode:9000` |
| `dnnm` | `profesorbigdata/hadoop-datanode-image-profesor:v2.0` | DataNode (guarda los bloques) + NodeManager (lanza contenedores YARN). Replicación 1 |
| `resourcemanager` | `profesorbigdata/hadoop-resourcemanager-image-profesor:v2.0` | Planificador de YARN |
| `client` | `profesorbigdata/hadoop-client-image-profesor:v2.0` | Jupyter con `hdfs`, `yarn` y `mapred` para las prácticas de Hadoop |
| `hdfs-init` | imagen del `client` | Crea `/user/hive/warehouse` y `/tmp/hive` en HDFS y termina |
| `minio` | `bitnamilegacy/minio:2025.4.22` | Object storage compatible con S3 |
| `minio-init` | `bitnamilegacy/minio-client:2025.4.16` | Crea los buckets `raw-data`, `processed`, `warehouse` y termina |
| `mysql-metastore` | `mysql:8.4` | Base de datos **sólo de metadatos** del catálogo |
| `hive-metastore` | `profesorbigdata/hive-image-profesor:v2.0` (Hive 4.0.1) | Catálogo compartido: esquema, formato y ruta de cada tabla (HDFS o S3) |
| `hiveserver2` | igual que el anterior | HiveQL por JDBC (`beeline`); ejecuta con Tez en modo local |
| `trino` | `trinodb/trino:476` | SQL distribuido: catálogos `hive` (HDFS + MinIO), `mysql` y `tpch` |
| `mysql-retail` | `mysql:8.4` | Origen operacional `retail_db` |
| `retail-init` | imagen del `lab` | Genera y carga `retail_db` (semilla fija: todos tienen los mismos datos) |
| `lab` | `profesorbigdata/lab-image-profesor:v2.0` | JupyterLab, DuckDB (Python y CLI), dlt, dbt, boto3, AWS CLI, cliente Trino |
| `postgres-airflow` | `postgres:16.8-bookworm` | Metadatos de Airflow |
| `airflow-init` | `profesorbigdata/airflow-image-profesor:v2.0` (Airflow 2.10.5) | Migra la base de datos y crea `admin/admin` |
| `airflow-webserver` / `airflow-scheduler` | igual que el anterior | Interfaz web y planificador (`LocalExecutor`) |

**¿Por qué dos MySQL?** Uno guarda *dónde apuntan las tablas* (catálogo) y el otro es *de dónde
vienen los datos* (origen). Mezclarlos confunde dos conceptos distintos.

**¿Por qué un Postgres para Airflow?** Airflow consulta y actualiza su estado continuamente; no
se mezcla con el catálogo ni con los datos, igual que en producción.

**`retail_db`** reproduce el esquema clásico de los cursos de Hadoop:

```mermaid
erDiagram
    departments ||--o{ categories : tiene
    categories ||--o{ products : agrupa
    customers ||--o{ orders : realiza
    orders ||--|{ order_items : contiene
    products ||--o{ order_items : aparece_en

    departments { int department_id PK
                  string department_name }
    categories  { int category_id PK
                  int category_department_id FK
                  string category_name }
    products    { int product_id PK
                  int product_category_id FK
                  string product_name
                  decimal product_price }
    customers   { int customer_id PK
                  string customer_fname
                  string customer_lname
                  string customer_city
                  string customer_state }
    orders      { int order_id PK
                  datetime order_date
                  int order_customer_id FK
                  string order_status }
    order_items { int order_item_id PK
                  int order_item_order_id FK
                  int order_item_product_id FK
                  int order_item_quantity
                  decimal order_item_subtotal }
```

6 departamentos · 24 categorías · 303 productos · 2.000 clientes · 20.000 pedidos · ~60.000 líneas.

---

## Recorrido 1: consultar el lago (`lake`)

```bash
docker compose -f docker-compose.stack.yml --profile lake up -d --build
docker exec -it trino trino
```

```sql
-- Tabla Parquet en HDFS
CREATE SCHEMA hive.demo
  WITH (location = 'hdfs://namenode:9000/user/hive/warehouse/demo.db');
CREATE TABLE hive.demo.nation WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.nation;
SELECT count(*) FROM hive.demo.nation;             -- 25

-- Tabla Parquet en MinIO
CREATE SCHEMA hive.minio WITH (location = 's3a://warehouse/minio.db');
CREATE TABLE hive.minio.region WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.region;
SELECT count(*) FROM hive.minio.region;            -- 5

-- Federación: el lago y MySQL en la misma consulta
SELECT c.customer_city, count(*) AS pedidos
FROM mysql.retail_db.orders o
JOIN mysql.retail_db.customers c ON o.order_customer_id = c.customer_id
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

El catálogo es **compartido**: lo creado desde Trino se ve desde HiveServer2.

```bash
docker exec -it hiveserver2 beeline -u "jdbc:hive2://localhost:10000/"
```

```sql
SHOW DATABASES;                       -- default, demo, minio
SELECT count(*) FROM demo.nation;     -- 25 (lee de HDFS)
SELECT count(*) FROM minio.region;    -- 5  (lee de MinIO)
```

YARN sigue disponible para MapReduce clásico. Las prácticas de `notebooks/` se lanzan desde el
Jupyter del `client` (http://localhost:8889), igual que en el clúster mínimo; el `client` arranca
con el profile `hadoop` o `all`:

```bash
docker compose -f docker-compose.stack.yml --profile hadoop up -d
```

---

## Recorrido 2: ELT con dlt, dbt y Airflow (`elt`)

```mermaid
flowchart LR
    subgraph origen["Origen"]
        MY[("mysql-retail<br/>retail_db")]
    end
    subgraph duck["/workspace/data/retail.duckdb"]
        RAW["retail_raw<br/>(copia tal cual)"]
        STG["main_staging<br/>stg_* (vistas)"]
        MART["main_marts<br/>ventas_por_ciudad<br/>ventas_por_departamento"]
    end
    MY -- "E + L<br/>dlt" --> RAW
    RAW -- "T<br/>dbt run" --> STG --> MART
    AF{{Airflow<br/>DAG elt_retail}} -. orquesta .-> MY
```

### A mano, desde el nodo `lab`

```bash
docker compose -f docker-compose.stack.yml --profile elt up -d --build

# EL: dlt replica mysql-retail -> DuckDB (esquema retail_raw)
docker exec lab python /workspace/dlt/mysql_to_duckdb.py

# T: dbt transforma de staging a marts, y comprueba los tests
docker exec -w /workspace/dbt/retail lab dbt run
docker exec -w /workspace/dbt/retail lab dbt test

# Resultado
docker exec lab duckdb /workspace/data/retail.duckdb \
  -c "SELECT * FROM main_marts.ventas_por_ciudad LIMIT 5;"
```

dlt **no transforma**: trae los datos tal cual. dbt **no extrae**: sólo transforma lo cargado.
Esa separación es lo que distingue ELT de ETL.

### Con Airflow

1. Abre http://localhost:8082 (`admin` / `admin`).
2. Activa el DAG **`elt_retail`** y pulsa *Trigger DAG*.
3. Verás tres tareas: `dlt_mysql_a_duckdb` → `dbt_run` → `dbt_test`.

```bash
# O desde la terminal
docker exec airflow-scheduler airflow dags unpause elt_retail
docker exec airflow-scheduler airflow dags trigger elt_retail
docker exec airflow-scheduler airflow dags list-runs -d elt_retail
```

> DuckDB admite **un solo proceso escribiendo**. Si un cuaderno del `lab` tiene abierto
> `retail.duckdb`, la tarea de Airflow fallará hasta que cierres la conexión.

### MinIO desde el `lab`

El `lab` trae las variables `AWS_*` apuntando a MinIO:

```bash
docker exec lab aws s3 ls
docker exec lab aws s3 cp /workspace/dbt/retail/dbt_project.yml s3://raw-data/
```

```python
import duckdb
con = duckdb.connect()
con.sql("""
  CREATE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
                       ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false);
""")
con.sql("COPY (SELECT * FROM range(10)) TO 's3://processed/rango.parquet'")
con.sql("SELECT * FROM 's3://processed/rango.parquet'").show()
```

---

## Operación diaria

| Acción | Comando |
|---|---|
| Apagar entre sesiones (conserva todo) | `docker compose -f docker-compose.stack.yml --profile all stop` |
| Volver a encender | `docker compose -f docker-compose.stack.yml --profile lake up -d --wait --wait-timeout 600` |
| Ver estado | `docker compose -f docker-compose.stack.yml --profile all ps -a` |
| Ver logs | `docker compose -f docker-compose.stack.yml logs -f hive-metastore trino` |
| Cambiar de escenario | `... --profile lake stop` y después `... --profile elt up -d` |
| Borrar contenedores y red (los datos quedan) | `docker compose -f docker-compose.stack.yml --profile all down` |
| **Reiniciar de cero** (borra HDFS, MinIO, MySQL...) | `docker compose -f docker-compose.stack.yml --profile all down -v` |
| Recargar `retail_db` | `docker compose -f docker-compose.stack.yml run --rm retail-init python /workspace/init/retail_init.py --force` |

> Usa **`stop`/`start`** en el día a día. `down` elimina contenedores y red; con `-v`, también los
> volúmenes con los datos.

---

## Acceso desde el aula

El stack del Ubuntu de casa no se ve desde el instituto: son dos redes distintas. No hay que abrir
el router ni el escritorio remoto (RDP) ni SSH. Los alumnos consultan solo los puertos del stack.

La red privada es [Tailscale](https://tailscale.com/), la misma IP `100.x` del escritorio remoto.
El procedimiento para repetirlo —casa en Ubuntu 24.04, política en el editor de grants, clave
`tag:aula`, Windows 10, Windows 11 y Ubuntu 22.04 LTS del aula— está en un único sitio:

**[Repetir el acceso de aula con Tailscale](https://iesataulfoargentasor.github.io/bigdata-stack-docker/#tailscale)**

Es un único stack compartido: un borrado en HDFS, MinIO o Airflow lo ve toda la clase.

---

## Estructura del repositorio

```text
bigdata-stack-docker/
├── docker-compose.stack.yml      Stack completo con profiles  ← el principal
├── docker-compose.yml            Clúster Hadoop mínimo (construye las imágenes)
├── docker-compose.hub.yml        Clúster Hadoop mínimo (imágenes de Docker Hub)
├── docker-compose.kafka.yml      Clúster Kafka de 3 nodos + Kafka UI
│
├── Base/                         Imagen base: Ubuntu 22.04 + Java 8 + Hadoop 3.3.6
├── NameNode/                     NameNode (+ core-site-stack.xml con proxyuser para Hive)
├── DataNode-NodeManager/         DataNode + NodeManager
├── ResourceManager/              ResourceManager
├── Client/                       Cliente Hadoop + Jupyter (:8889)
│
├── Hive/                         Hive 4.0.1 + driver MySQL + S3A
│   └── conf/                     hive-site.xml, core-site.xml, hdfs-site.xml
├── Trino/
│   ├── catalog/                  hive, mysql, tpch
│   ├── hadoop/                   core-site y hdfs-site para leer HDFS
│   └── jvm.config
├── Lab/                          JupyterLab + DuckDB + dlt + dbt + AWS CLI
├── Airflow/
│   ├── Dockerfile                Airflow 2.10.5 + venv con dlt y dbt
│   └── dags/elt_retail.py        dlt → dbt run → dbt test
│
├── workspace/                    Montado en /workspace (lab y Airflow)
│   ├── init/retail_init.py       Generador de retail_db
│   ├── dlt/mysql_to_duckdb.py    EL
│   ├── dbt/retail/               T: staging y marts
│   └── data/                     retail.duckdb (generado, no se versiona)
│
├── notebooks/                    Prácticas MapReduce del cliente Hadoop
└── docs/                         Página de GitHub Pages (guía de despliegue)
```

---

## Decisiones técnicas

Problemas reales encontrados al montar el stack y cómo se resolvieron:

| Situación | Solución |
|---|---|
| MinIO dejó de publicar imágenes en Docker Hub y quay.io | Se usa `bitnamilegacy/minio` y `bitnamilegacy/minio-client` (misma versión de abril de 2025) |
| La imagen oficial de Hive no trae el driver de MySQL ni S3A en el classpath | `Hive/Dockerfile` añade `mysql-connector-j` y enlaza `hadoop-aws` + `aws-java-sdk-bundle` de `tools/lib` |
| HiveServer2: *"User hdadmin is not allowed to impersonate hdadmin"* | El NameNode del stack monta `core-site-stack.xml` con `hadoop.proxyuser.hdadmin.*` (la imagen original no cambia) |
| Hive no pasa `fs.s3a.access.key` a Tez (`hive.conf.hidden.list`) | Las credenciales de MinIO van como `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` |
| HiveServer2 no rearranca tras `stop`/`start` (*"running as process 7"*) | El `.pid` va a un `tmpfs` (`HIVESERVER2_PID_DIR`) |
| Airflow no podía escribir el `retail.duckdb` creado por el `lab` | `lab` y Airflow corren con el mismo uid (`AIRFLOW_UID`, 50000 por defecto) |
| dlt/dbt chocan con las dependencias de Airflow | Van en un venv aparte dentro de la imagen de Airflow (`/opt/elt-venv`) |
| Permisos de HDFS para Hive y Trino | Acceden como `hdadmin`, superusuario de HDFS (`HADOOP_USER_NAME`) |
| Con todo arrancando a la vez, el NameNode tarda > 2 min | Healthchecks de Hadoop con 60 reintentos |

---

## Solución de problemas

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `docker compose up` no arranca nada | Falta el profile | Añade `--profile lake`, `elt` o `all` |
| `Conflict. The container name "/namenode" is already in use` | Está levantado `docker-compose.yml` (clúster mínimo) | `docker compose down` en ese compose y repite |
| `Ports are not available` | Otra aplicación ocupa el puerto | Revisa el puerto indicado: Kafka UI usa 8083 y Trino 8080 |
| `Ports are not available: ... 3306` | Hay un MySQL instalado en el equipo | Páralo o cambia el puerto publicado de `mysql-metastore` |
| `namenode` *unhealthy* / `dependency failed to start` | Revisa los logs: puede faltar tiempo o existir otro fallo | Consulta los logs de namenode, dnnm y hdfs-init antes de repetir el arranque |
| Trino: `hive-metastore:9083: Read timed out` | Metastore aún arrancando o equipo saturado | `docker compose ... ps` y reintenta cuando esté *healthy* |
| Trino: `Partition location does not exist` tras un error | Un `CREATE TABLE AS` se interrumpió | `DROP TABLE` y vuelve a crearla |
| Tarea de Airflow: `Cannot open file retail.duckdb` | Lo tiene abierto un cuaderno del `lab` | Cierra la conexión de DuckDB en el cuaderno |
| Docker Desktop devuelve *Internal Server Error* | El motor no responde; puede haber problemas de WSL, disco o almacenamiento | Comprueba `docker version`, el espacio libre y los diagnósticos de Docker Desktop. No restablezcas ni borres volúmenes sin copia |
| Contenedores de Hadoop en `Exited (137)` tras `stop` | Los demonios no se cierran en 10 s | Normal; `start` los recupera sin perder datos |

---

## Otros compose del repositorio

| Fichero | Qué levanta | Comando |
|---|---|---|
| `docker-compose.yml` | Clúster Hadoop mínimo, construyendo las imágenes | `docker compose up -d --build` |
| `docker-compose.hub.yml` | El mismo clúster con las imágenes de Docker Hub | `docker compose -f docker-compose.hub.yml up -d` |
| `docker-compose.kafka.yml` | Kafka 4 (KRaft, 3 nodos) + Kafka UI en :8083 | `docker compose -f docker-compose.kafka.yml up -d` |

> El clúster mínimo y el stack usan los **mismos nombres de contenedor**: no los levantes a la vez.

---

## Créditos

- Clúster Hadoop y adaptación: IES Ataúlfo Argenta — profesorado de Big Data Aplicado.
