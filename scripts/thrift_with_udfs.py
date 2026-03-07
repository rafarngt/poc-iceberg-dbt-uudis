"""
Spark Thrift Server con UDFs de Surrogate Keys pre-registradas.

Arranca el HiveThriftServer2 programáticamente dentro del mismo
SparkContext donde se registran uuid7() y uuid5_sk().

Esto resuelve el problema de que spark-submit y start-thriftserver.sh
crean procesos Spark separados (las UDFs no se comparten).

UDFs registradas:
  uuid7()          → UUID v7 time-ordered (uuid-utils, Rust)
  uuid5_sk(seed)   → UUID v5 deterministico (stdlib Python)
"""

import time
import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# ── SparkSession (hereda config de spark-defaults.conf) ────────
spark = (
    SparkSession.builder
    .appName("ThriftServerWithUDFs")
    .enableHiveSupport()
    .getOrCreate()
)

# ── Registrar UDFs ─────────────────────────────────────────────

@udf(returnType=StringType())
def _uuid7_udf():
    import uuid_utils
    return str(uuid_utils.uuid7())

@udf(returnType=StringType())
def _uuid5_sk_udf(seed: str):
    import uuid
    ns = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    return str(uuid.uuid5(ns, seed or ""))

spark.udf.register("uuid7", _uuid7_udf)
spark.udf.register("uuid5_sk", _uuid5_sk_udf)

# Verificar que funcionan
row = spark.sql("""
    SELECT
        uuid7()                      AS sk_scd1,
        uuid5_sk('cd004|2024-03-10') AS sk_scd2
""").collect()[0]
print(f"UDF uuid7()    OK → {row['sk_scd1']}")
print(f"UDF uuid5_sk() OK → {row['sk_scd2']}")

# ── Crear schemas necesarios ───────────────────────────────────
for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    print(f"Schema '{schema}' OK")

# ── Inicializar tablas ACID de Hive en Derby ───────────────────
# Iceberg tipo=hive usa MetastoreLock que requiere HIVE_LOCKS,
# TXNS y NEXT_LOCK_ID en Derby. Spark solo crea el esquema básico.
# Las creamos aquí vía JDBC antes de que el Thrift Server empiece.
def _init_derby_acid_tables():
    """
    Crea las tablas ACID de Hive en Derby que MetastoreLock necesita.
    Spark solo crea el esquema básico de metastore (TBLS, DBS, etc.)
    pero NO las tablas de transacciones/locks de Hive ACID.
    """
    try:
        jvm = spark._jvm
        conn = jvm.java.sql.DriverManager.getConnection(
            "jdbc:derby:/dbt_project/metastore_db"
        )
        conn.setAutoCommit(True)
        stmt = conn.createStatement()

        # Esquema ACID completo de Hive 2.3.x para Derby
        # (MetastoreLock de Iceberg usa: TXNS, HIVE_LOCKS, AUX_TABLE,
        #  NEXT_TXN_ID, NEXT_LOCK_ID y tablas auxiliares de TxnHandler)
        ddls = [
            ("TXNS", """
                CREATE TABLE TXNS (
                    TXN_ID              BIGINT NOT NULL,
                    TXN_STATE           CHAR(1) NOT NULL,
                    TXN_STARTED         BIGINT NOT NULL,
                    TXN_LAST_HEARTBEAT  BIGINT NOT NULL,
                    TXN_USER            VARCHAR(128) NOT NULL,
                    TXN_HOST            VARCHAR(128) NOT NULL,
                    TXN_AGENT_INFO      VARCHAR(128),
                    TXN_META_INFO       VARCHAR(128),
                    TXN_HEARTBEAT_COUNT INTEGER,
                    TXN_TYPE            INTEGER,
                    PRIMARY KEY (TXN_ID)
                )
            """),
            ("NEXT_TXN_ID",
             "CREATE TABLE NEXT_TXN_ID (NTXN_NEXT BIGINT NOT NULL)"),
            ("TXN_COMPONENTS", """
                CREATE TABLE TXN_COMPONENTS (
                    TC_TXNID          BIGINT NOT NULL,
                    TC_DATABASE       VARCHAR(128) NOT NULL,
                    TC_TABLE          VARCHAR(256),
                    TC_PARTITION      VARCHAR(767),
                    TC_OPERATION_TYPE CHAR(1) NOT NULL
                )
            """),
            ("COMPLETED_TXN_COMPONENTS", """
                CREATE TABLE COMPLETED_TXN_COMPONENTS (
                    CTC_TXNID     BIGINT NOT NULL,
                    CTC_DATABASE  VARCHAR(128) NOT NULL,
                    CTC_TABLE     VARCHAR(256),
                    CTC_PARTITION VARCHAR(767)
                )
            """),
            ("HIVE_LOCKS", """
                CREATE TABLE HIVE_LOCKS (
                    HL_LOCK_EXT_ID      BIGINT NOT NULL,
                    HL_LOCK_INT_ID      BIGINT NOT NULL,
                    HL_TXNID            BIGINT,
                    HL_DB               VARCHAR(128) NOT NULL,
                    HL_TABLE            VARCHAR(256),
                    HL_PARTITION        VARCHAR(767),
                    HL_LOCK_STATE       CHAR(1) NOT NULL,
                    HL_LOCK_TYPE        CHAR(1) NOT NULL,
                    HL_LAST_HEARTBEAT   BIGINT NOT NULL,
                    HL_ACQUIRED_AT      BIGINT,
                    HL_USER             VARCHAR(128) NOT NULL,
                    HL_HOST             VARCHAR(128) NOT NULL,
                    HL_HEARTBEAT_COUNT  INTEGER,
                    HL_AGENT_INFO       VARCHAR(128),
                    HL_BLOCKEDBY_EXT_ID BIGINT,
                    HL_BLOCKEDBY_INT_ID BIGINT,
                    PRIMARY KEY (HL_LOCK_EXT_ID, HL_LOCK_INT_ID)
                )
            """),
            ("NEXT_LOCK_ID",
             "CREATE TABLE NEXT_LOCK_ID (NL_NEXT BIGINT NOT NULL)"),
            ("AUX_TABLE", """
                CREATE TABLE AUX_TABLE (
                    MT_KEY1   VARCHAR(128) NOT NULL,
                    MT_KEY2   BIGINT NOT NULL,
                    MT_COMMENT VARCHAR(255),
                    PRIMARY KEY (MT_KEY1, MT_KEY2)
                )
            """),
            ("COMPACTION_QUEUE", """
                CREATE TABLE COMPACTION_QUEUE (
                    CQ_ID             BIGINT NOT NULL,
                    CQ_DATABASE       VARCHAR(128) NOT NULL,
                    CQ_TABLE          VARCHAR(256) NOT NULL,
                    CQ_PARTITION      VARCHAR(767),
                    CQ_STATE          CHAR(1) NOT NULL,
                    CQ_TYPE           CHAR(1) NOT NULL,
                    CQ_TBLPROPERTIES  VARCHAR(2048),
                    CQ_WORKER_ID      VARCHAR(128),
                    CQ_START          BIGINT,
                    CQ_RUN_AS         VARCHAR(128),
                    CQ_HIGHEST_TXN_ID BIGINT,
                    CQ_META_INFO      BLOB,
                    CQ_HADOOP_JOB_ID  VARCHAR(128),
                    PRIMARY KEY (CQ_ID)
                )
            """),
            ("COMPLETED_COMPACTIONS", """
                CREATE TABLE COMPLETED_COMPACTIONS (
                    CC_ID             BIGINT NOT NULL,
                    CC_DATABASE       VARCHAR(128) NOT NULL,
                    CC_TABLE          VARCHAR(256) NOT NULL,
                    CC_PARTITION      VARCHAR(767),
                    CC_STATE          CHAR(1) NOT NULL,
                    CC_TYPE           CHAR(1) NOT NULL,
                    CC_TBLPROPERTIES  VARCHAR(2048),
                    CC_WORKER_ID      VARCHAR(128),
                    CC_START          BIGINT,
                    CC_END            BIGINT,
                    CC_RUN_AS         VARCHAR(128),
                    CC_HIGHEST_TXN_ID BIGINT,
                    CC_META_INFO      BLOB,
                    CC_HADOOP_JOB_ID  VARCHAR(128),
                    PRIMARY KEY (CC_ID)
                )
            """),
            ("NEXT_COMPACTION_QUEUE_ID",
             "CREATE TABLE NEXT_COMPACTION_QUEUE_ID (NCQ_NEXT BIGINT NOT NULL)"),
            ("WRITE_SET", """
                CREATE TABLE WRITE_SET (
                    WS_DATABASE       VARCHAR(128) NOT NULL,
                    WS_TABLE          VARCHAR(256) NOT NULL,
                    WS_PARTITION      VARCHAR(767),
                    WS_TXNID          BIGINT NOT NULL,
                    WS_COMMIT_ID      BIGINT NOT NULL,
                    WS_OPERATION_TYPE CHAR(1) NOT NULL
                )
            """),
        ]

        for table_name, ddl in ddls:
            try:
                stmt.execute(ddl.strip())
                print(f"  Derby tabla creada: {table_name}")
            except Exception as e:
                msg = str(e)
                if "X0Y32" in msg or "already exists" in msg.lower():
                    print(f"  Derby tabla ya existe: {table_name}")
                else:
                    print(f"  Derby tabla {table_name} warning: {msg[:120]}")

        # Seed de secuencias (idempotente: falla si ya hay datos)
        for seed_sql in [
            "INSERT INTO NEXT_TXN_ID VALUES(1)",
            "INSERT INTO NEXT_LOCK_ID VALUES(1)",
            "INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1)",
        ]:
            try:
                stmt.execute(seed_sql)
            except Exception:
                pass  # ya tiene datos

        stmt.close()
        conn.close()
        print("Derby ACID tables OK")
    except Exception as e:
        print(f"Derby ACID init warning (non-fatal): {e}")

_init_derby_acid_tables()

# ── Arrancar Thrift Server en este SparkContext ────────────────
print("Starting HiveThriftServer2 on port 10000...")

jvm = spark._jvm

# Configurar HiveConf para el Thrift Server
hive_conf = jvm.org.apache.hadoop.hive.conf.HiveConf()
hive_conf.set("hive.server2.thrift.port", "10000")
hive_conf.set("hive.server2.thrift.bind.host", "0.0.0.0")
hive_conf.set("hive.server2.authentication", "NOSASL")

# En Spark 3.5, startWithContext espera un SQLContext (no SparkSession)
sql_ctx = spark._wrapped if hasattr(spark, '_wrapped') else spark._jsparkSession
hive_server = jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2

# Crear el SQLContext Java que el Thrift Server necesita
java_sql_ctx = jvm.org.apache.spark.sql.SQLContext(spark._jsc.sc())
hive_server.startWithContext(java_sql_ctx)

print("HiveThriftServer2 started. Waiting for connections...")

# ── Mantener el proceso vivo ───────────────────────────────────
def shutdown(sig, frame):
    print("Shutting down...")
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

# Bloquear el proceso principal
while True:
    time.sleep(60)
