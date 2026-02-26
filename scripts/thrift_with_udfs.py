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

# ── Arrancar Thrift Server en este SparkContext ────────────────
print("Starting HiveThriftServer2 on port 10000...")

# Acceso directo al JVM para arrancar el Thrift Server
jvm = spark._jvm
hive_server = jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
hive_ctx = spark._jsparkSession

# startWithContext espera (SQLContext, HiveConf)
hive_conf = jvm.org.apache.hadoop.hive.conf.HiveConf()
hive_conf.set("hive.server2.thrift.port", "10000")
hive_conf.set("hive.server2.thrift.bind.host", "0.0.0.0")
hive_conf.set("hive.server2.authentication", "NOSASL")

hive_server.startWithContext(hive_ctx)

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
