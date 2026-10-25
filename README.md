# Stack de Big Data en Docker

Ecosistema completo de ingeniería de datos para clase, levantado con Docker Compose:
**HDFS + YARN**, **MinIO**, **Hive Metastore + HiveServer2**, **Trino**, un nodo de trabajo
con **DuckDB + dlt + dbt** y **Airflow** para orquestar.

Parte del clúster Hadoop del profesor (imágenes `profesorbigdata/*:v2.0`) y añade las piezas del
[stack docente de Aitor Medrano](https://aitor-medrano.github.io/iabd/de/stack-docker-de.html).

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
10. [Acceso desde el aula](#acceso-desde-el-aula)
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

El arranque está encadenado con `healthcheck` y `depends_on`: cada servicio espera a que el
anterior esté **realmente** listo.

```mermaid
flowchart LR
    subgraph lake["profile lake"]
        direction LR
        nn[namenode] --> dnnm --> hi[hdfs-init] --> hms[hive-metastore] --> hs2[hiveserver2]
        rm[resourcemanager] --> dnnm
        mym[mysql-metastore] --> hms
        hms --> tr[trino]
        mi[minio] --> mii[minio-init] --> tr
        myr[mysql-retail] --> tr
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
sólo se enciende lo que se necesita y se ahorra memoria. Un `docker compose up` a secas no
arranca nada.

### Profiles de componente

| Profile | Servicios | Para qué |
|---|---|---|
| `hadoop` | namenode, resourcemanager, dnnm, client, hdfs-init | HDFS + YARN (+ Jupyter con cliente Hadoop) |
| `minio` | minio, minio-init | Object storage (el lago "moderno") |
| `hive` | mysql-metastore, hive-metastore, hiveserver2 | Catálogo de Hive |
| `trino` | trino | Motor de consulta sobre el lago |
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

```bash
git clone https://github.com/iesataulfoargentasor/bigdata-stack-docker.git
cd bigdata-stack-docker

# Escenario "lago": HDFS + YARN + MinIO + Hive + Trino
docker compose -f docker-compose.stack.yml --profile lake up -d

# Escenario "ELT": MinIO + lab (DuckDB/dlt/dbt) + Airflow
docker compose -f docker-compose.stack.yml --profile elt up -d

# Estado
docker compose -f docker-compose.stack.yml --profile all ps -a
```

La **primera vez** descarga las imágenes de Docker Hub (`profesorbigdata/*:v2.0`, unos 12 GB
entre los dos escenarios) y tarda según la conexión. Las siguientes veces arranca en 2-3 minutos.
`--build` sólo hace falta si has cambiado un Dockerfile y quieres reconstruir en local.

> **Atajo para no escribir `-f` siempre:**
> PowerShell: `$env:COMPOSE_FILE="docker-compose.stack.yml"` ·
> bash: `export COMPOSE_FILE=docker-compose.stack.yml`.
> A partir de ahí basta con `docker compose --profile lake up -d`.

La guía detallada, con comprobaciones en cada paso, está en
**[GitHub Pages](https://iesataulfoargentasor.github.io/bigdata-stack-docker/)**.

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
| Volver a encender | `docker compose -f docker-compose.stack.yml --profile lake start` |
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

El stack publicado en el equipo de casa no es visible desde el instituto: son dos redes distintas.
Tampoco hay que abrir los puertos en el router. Las credenciales son las de clase y un puerto
abierto a Internet deja HDFS, MinIO y Airflow al alcance de cualquiera.

[Tailscale](https://tailscale.com/) crea una red privada entre ese Ubuntu y los PCs del aula.
Funciona con CG-NAT y cuando el instituto sólo permite salir por el 443. Los alumnos usan las
mismas consolas, con la IP Tailscale del equipo de casa en lugar de `localhost`.

En el Ubuntu de casa, si Tailscale ya está instalado (por ejemplo para el escritorio remoto),
no lo reinstales:

```bash
tailscale status
tailscale ip -4
```

Esa IP es la misma del escritorio remoto. Antes de repartir el acceso, cambia la política de
Tailscale: la de por defecto (`"src": ["*"]`) dejaría el puerto 3389 abierto a los alumnos. La
política y la auth key con etiqueta `tag:aula` están en la
[guía](https://iesataulfoargentasor.github.io/bigdata-stack-docker/#aula). Tus equipos siguen
entrando a todo; los del aula solo a los puertos del stack.

Windows 11, en PowerShell como administrador:

```powershell
winget install tailscale.tailscale
tailscale up --auth-key=tskey-auth-XXXXXXXX --unattended
```

Ubuntu 22.04:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --auth-key=tskey-auth-XXXXXXXX
```

| Consola | Desde el aula |
|---|---|
| NameNode | `http://100.x.x.x:9870` |
| YARN | `http://100.x.x.x:8088` |
| MinIO | `http://100.x.x.x:9001` (`minioadmin` / `minioadmin`) |
| Trino | `http://100.x.x.x:8080` |
| HiveServer2 | `http://100.x.x.x:10002`, JDBC en `:10000` |
| JupyterLab / Jupyter Hadoop | `:8888` y `:8889` |
| Airflow | `http://100.x.x.x:8082` (`admin` / `admin`) |
| MySQL `retail_db` / catálogo | `:3307` (`retail`) y `:3306` (`hive`) |

Deja el equipo de casa encendido y sin suspensión durante la clase. Es un único stack compartido:
un `DROP` o un borrado en HDFS afecta a todo el grupo.

Los pasos, incluida la suspensión de Ubuntu, están en la
[guía de despliegue](https://iesataulfoargentasor.github.io/bigdata-stack-docker/#aula).

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
| `Ports are not available: ... 8080` | Kafka UI u otra app usa el 8080 | Para `docker-compose.kafka.yml` o la app que lo ocupe |
| `Ports are not available: ... 3306` | Hay un MySQL instalado en el equipo | Páralo o cambia el puerto publicado de `mysql-metastore` |
| `namenode` *unhealthy* / `dependency failed to start` | Equipo lento o con poca memoria | Espera y repite el mismo `up -d`; levanta `lake` y `elt` por separado |
| Trino: `hive-metastore:9083: Read timed out` | Metastore aún arrancando o equipo saturado | `docker compose ... ps` y reintenta cuando esté *healthy* |
| Trino: `Partition location does not exist` tras un error | Un `CREATE TABLE AS` se interrumpió | `DROP TABLE` y vuelve a crearla |
| Tarea de Airflow: `Cannot open file retail.duckdb` | Lo tiene abierto un cuaderno del `lab` | Cierra la conexión de DuckDB en el cuaderno |
| Docker Desktop devuelve *Internal Server Error* | Disco lleno | Libera espacio o mueve el disco de Docker a otra unidad |
| Contenedores de Hadoop en `Exited (137)` tras `stop` | Los demonios no se cierran en 10 s | Normal; `start` los recupera sin perder datos |

---

## Otros compose del repositorio

| Fichero | Qué levanta | Comando |
|---|---|---|
| `docker-compose.yml` | Clúster Hadoop mínimo, construyendo las imágenes | `docker compose up -d --build` |
| `docker-compose.hub.yml` | El mismo clúster con las imágenes de Docker Hub | `docker compose -f docker-compose.hub.yml up -d` |
| `docker-compose.kafka.yml` | Kafka 4 (KRaft, 3 nodos) + Kafka UI en :8080 | `docker compose -f docker-compose.kafka.yml up -d` |

> El clúster mínimo y el stack usan los **mismos nombres de contenedor**: no los levantes a la vez.

---

## Créditos

- Arquitectura y profiles inspirados en
  [*Stack Docker para Ingeniería de datos*](https://aitor-medrano.github.io/iabd/de/stack-docker-de.html),
  de Aitor Medrano.
- Clúster Hadoop y adaptación: IES Ataúlfo Argenta — profesorado de Big Data Aplicado.
