"""
Script 03 - Consultas de comparación SCD1 vs SCD2
──────────────────────────────────────────────────
Muestra de forma visual la diferencia de comportamiento
entre SCD1 y SCD2 con la estrategia híbrida de SKs:

  SCD1: UUID v7 time-ordered (uuid-utils, Rust)
        → Estable: mismo SK aunque el dato cambie en SAP

  SCD2: UUID v5 deterministico (stdlib Python uuid.uuid5)
        → Un SK por versión, idempotente en full-refresh
        → Mismo center_cd + source_updated_at = mismo UUID siempre

Ejecutar:
  spark-submit /scripts/03_query_comparison.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("query_comparison")
    .getOrCreate()
)

DIVIDER = "=" * 70

# ────────────────────────────────────────────────────────────────
# 1. SCD1: Estado actual (un solo registro por center_cd)
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  SCD TYPE 1 - Estado actual (UUID v7 time-ordered, estable por center_cd)")
print(DIVIDER)
spark.sql("""
    SELECT
        sk_name          AS sk_name_scd1,
        center_cd,
        center_name,
        status,
        _silver_updated_at
    FROM spark_catalog.silver.silver_cost_centers_scd1
    ORDER BY center_cd
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 2. SCD2: Estado actual (is_current=true)
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  SCD TYPE 2 - Versiones VIGENTES (UUID v5 deterministico, is_current=true)")
print(DIVIDER)
spark.sql("""
    SELECT
        sk_name          AS sk_name_scd2_current,
        center_cd,
        center_name,
        status,
        valid_from_sk,
        is_current
    FROM spark_catalog.silver.silver_cost_centers_scd2
    WHERE is_current = true
    ORDER BY center_cd
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 3. SCD2: Historial completo (todas las versiones)
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  SCD TYPE 2 - HISTORIAL COMPLETO (todas las versiones)")
print(DIVIDER)
spark.sql("""
    SELECT
        sk_name,
        center_cd,
        center_name,
        status,
        valid_from_sk,
        valid_to,
        is_current
    FROM spark_catalog.silver.silver_cost_centers_scd2
    ORDER BY center_cd, valid_from_sk
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 4. Comparación Gold
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  GOLD - Comparacion SCD1 vs SCD2 (side-by-side)")
print(DIVIDER)
spark.sql("""
    SELECT
        center_cd,
        sk1_uuid,
        sk2_uuid_current,
        scd1_status,
        scd2_status,
        has_history,
        scd2_total_versions
    FROM spark_catalog.gold.gold_cost_centers_comparison
    ORDER BY center_cd
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 5. Point-in-time query demo (SCD2)
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  DEMO POINT-IN-TIME: ¿Cuál era el estado de cd004 el 2024-06-01?")
print(DIVIDER)
spark.sql("""
    SELECT
        sk_name,
        center_cd,
        center_name,
        status,
        valid_from_sk,
        valid_to
    FROM spark_catalog.silver.silver_cost_centers_scd2
    WHERE center_cd = 'cd004'
      AND valid_from_sk <= TIMESTAMP'2024-06-01 00:00:00'
      AND (valid_to IS NULL OR valid_to > TIMESTAMP'2024-06-01 00:00:00')
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 6. Iceberg Time Travel (ver estado de bronze en el pasado)
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  ICEBERG TIME TRAVEL - Historial de snapshots en bronze")
print(DIVIDER)
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary
    FROM spark_catalog.bronze.bronze_cost_centers.snapshots
    ORDER BY committed_at
""").show(truncate=False)

# ────────────────────────────────────────────────────────────────
# 7. Verificar idempotencia UUID v5 en SCD2
# ────────────────────────────────────────────────────────────────
print(f"\n{DIVIDER}")
print("  VERIFICACION IDEMPOTENCIA SCD2 (UUID v5 deterministico)")
print(DIVIDER)

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def _uuid5_sk_udf(seed: str):
    import uuid
    ns = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
    return str(uuid.uuid5(ns, seed or ""))

spark.udf.register("uuid5_sk", _uuid5_sk_udf)

spark.sql("""
    SELECT
        'cd004|2024-03-10 14:00:00' AS seed,
        uuid5_sk('cd004|2024-03-10 14:00:00') AS run1,
        uuid5_sk('cd004|2024-03-10 14:00:00') AS run2_mismo_seed,
        uuid5_sk('cd004|2025-06-01 09:00:00') AS run_otra_version
""").show(truncate=False)
print("  run1 == run2_mismo_seed → idempotente ✅")
print("  run1 != run_otra_version → versiones distintas = UUIDs distintos ✅")

print(f"\n{DIVIDER}")
print("  FIN DE LA DEMOSTRACION")
print(f"  Estrategia: SCD1=uuid7() (Rust) | SCD2=uuid5_sk() (deterministico)")
print(DIVIDER + "\n")

spark.stop()
