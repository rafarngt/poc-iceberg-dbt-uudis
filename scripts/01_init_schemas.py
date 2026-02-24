"""
Script 01 - Inicializar namespaces (bases de datos) en Iceberg
            y registrar UDFs de Surrogate Keys

Estrategia híbrida de SKs registrada aquí:
  uuid7()        → SCD1: UUID v7 time-ordered (uuid-utils, Rust)
  uuid5_sk(seed) → SCD2: UUID v5 deterministico (stdlib Python)

Ejecutar: spark-submit /scripts/01_init_schemas.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = (
    SparkSession.builder
    .appName("init_schemas")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
    .config("spark.sql.defaultCatalog", "iceberg")
    .getOrCreate()
)

# ── Namespaces ───────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.default")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.raw")

print("✓ Namespaces creados: iceberg.bronze, iceberg.silver, iceberg.gold, iceberg.raw")

# ── UDFs de Surrogate Keys ───────────────────────────────────
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def _uuid7_udf():
    import uuid_utils
    return str(uuid_utils.uuid7())

@udf(returnType=StringType())
def _uuid5_sk_udf(seed: str):
    import uuid
    ns = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    return str(uuid.uuid5(ns, seed or ""))

spark.udf.register("uuid7",    _uuid7_udf)
spark.udf.register("uuid5_sk", _uuid5_sk_udf)

# Verificación rápida
row = spark.sql("""
    SELECT
        uuid7()                      AS sk_scd1,
        uuid5_sk('cd004|2024-03-10') AS sk_scd2_v1,
        uuid5_sk('cd004|2024-03-10') AS sk_scd2_v1_repeat
""").collect()[0]

assert row["sk_scd2_v1"] == row["sk_scd2_v1_repeat"], "uuid5_sk no es idempotente!"
print(f"✓ uuid7()     registrado  → ejemplo: {row['sk_scd1']}")
print(f"✓ uuid5_sk()  registrado  → ejemplo: {row['sk_scd2_v1']} (idempotente ✅)")

spark.stop()
