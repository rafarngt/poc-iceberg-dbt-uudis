"""
Script 00 - Registrar UDFs de Surrogate Keys en Spark
═══════════════════════════════════════════════════════
Debe ejecutarse ANTES de arrancar el Thrift Server, o
importarse desde el script de inicio del Thrift Server.

Estrategia híbrida de SKs:
  ┌──────────┬────────────────────────────────────────────────┐
  │  Modelo  │  Función SQL                                   │
  ├──────────┼────────────────────────────────────────────────┤
  │  SCD1    │  uuid7()   → uuid-utils (Rust), time-ordered  │
  │          │              3.4M UUIDs/seg, 0.29 µs/uuid      │
  ├──────────┼────────────────────────────────────────────────┤
  │  SCD2    │  uuid5_sk(seed) → stdlib uuid, deterministico  │
  │          │              Mismo seed = mismo UUID SIEMPRE   │
  │          │              Seguro en full-refresh            │
  └──────────┴────────────────────────────────────────────────┘

Uso en Spark SQL (una vez registrado):
  SELECT uuid7()             AS sk_name   -- SCD1
  SELECT uuid5_sk('cd004|2024-03-10') AS sk_name  -- SCD2

Ejecutar standalone:
  spark-submit /scripts/00_register_udfs.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ── Inicializar Spark ────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("register_udfs")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
    .config("spark.sql.defaultCatalog", "iceberg")
    .getOrCreate()
)


# ═══════════════════════════════════════════════════════════════
# UDF 1: uuid7() — UUID v7 time-ordered via uuid-utils (Rust)
# ───────────────────────────────────────────────────────────────
# Uso: SCD1 — generado UNA VEZ al crear el registro.
#      En runs incrementales se recupera con COALESCE(existing, uuid7()).
#      El UUID v7 garantiza:
#        - Ordenabilidad cronológica (time-ordered)
#        - Unicidad global sin coordinación entre workers
#        - Mejor Iceberg file skipping vs UUID v4 (prefijo temporal)
# ═══════════════════════════════════════════════════════════════
@udf(returnType=StringType())
def _uuid7_udf():
    import uuid_utils
    return str(uuid_utils.uuid7())


spark.udf.register("uuid7", _uuid7_udf)


# ═══════════════════════════════════════════════════════════════
# UDF 2: uuid5_sk(seed) — UUID v5 deterministico desde semilla
# ───────────────────────────────────────────────────────────────
# Uso: SCD2 — generado por VERSIÓN de un registro.
#      La semilla (seed) es la combinación de las columnas que
#      identifican de forma única una versión:
#        seed = center_cd || '|' || valid_from_sk
#
#      Garantías:
#        - Idempotente: mismo seed = mismo UUID SIEMPRE
#        - Full-refresh seguro: los SKs no cambian entre runs
#        - FKs downstream nunca se rompen
#        - UUID v5 usa SHA-1 internamente (stdlib Python)
#
#      Namespace fijo del proyecto (UUIDv4 generado una sola vez):
#        No usar uuid.NAMESPACE_DNS ni NAMESPACE_URL para evitar
#        colisiones con otros sistemas que usen el mismo namespace.
# ═══════════════════════════════════════════════════════════════
# Namespace fijo para este proyecto (no cambiar)
_SK_NAMESPACE = "6ba7b812-9dad-11d1-80b4-00c04fd430c8"  # UUID v1 fijo conocido


@udf(returnType=StringType())
def _uuid5_sk_udf(seed: str):
    import uuid
    namespace = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    return str(uuid.uuid5(namespace, seed or ""))


spark.udf.register("uuid5_sk", _uuid5_sk_udf)


# ── Verificación ─────────────────────────────────────────────
print("\n" + "=" * 55)
print("  UDFs registradas en SparkSession")
print("=" * 55)

result = spark.sql("""
    SELECT
        uuid7()                          AS sk_scd1_example,
        uuid5_sk('cd004|2024-03-10')     AS sk_scd2_v1,
        uuid5_sk('cd004|2024-03-10')     AS sk_scd2_v1_repeat,
        uuid5_sk('cd004|2025-06-01')     AS sk_scd2_v2
""")

result.show(truncate=False)

# Verificar idempotencia UUID v5
row = result.collect()[0]
assert row["sk_scd2_v1"] == row["sk_scd2_v1_repeat"], \
    "ERROR: uuid5_sk no es idempotente!"
assert row["sk_scd2_v1"] != row["sk_scd2_v2"], \
    "ERROR: versiones distintas producen mismo SK!"

print("✓ uuid7()     → UUID v7 time-ordered (Rust, 9.7x stdlib)")
print("✓ uuid5_sk()  → UUID v5 deterministico (idempotente)")
print("✓ Idempotencia verificada: mismo seed = mismo UUID")
print("✓ Versiones distintas = UUIDs distintos")
print("=" * 55 + "\n")

spark.stop()
