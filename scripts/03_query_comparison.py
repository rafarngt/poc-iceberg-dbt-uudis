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
  python3 /scripts/03_query_comparison.py
  (desde spark_thrift_server o dbt_runner)
"""

from pyhive import hive

HOST = "spark-thrift"
DIVIDER = "=" * 70

conn = hive.connect(HOST, port=10000, auth="NOSASL")
cur  = conn.cursor()


def show(title, query, max_col=28):
    """Ejecuta la query y muestra resultado tabulado."""
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)
    cur.execute(query)
    rows = cur.fetchall()
    if cur.description:
        headers = [d[0].split(".")[-1] for d in cur.description]
        widths  = [max(max_col, len(h)) for h in headers]
        # Ajustar ancho al contenido real
        for row in rows:
            for i, v in enumerate(row):
                widths[i] = min(max_col, max(widths[i], len(str(v) if v is not None else "NULL")))
        fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
        print(fmt.format(*headers))
        print("  " + "-" * (sum(widths) + 2 * len(widths)))
        for row in rows:
            vals = [str(v)[:max_col] if v is not None else "NULL" for v in row]
            print(fmt.format(*vals))
    print(f"  ({len(rows)} rows)")


# ────────────────────────────────────────────────────────────────
# 1. SCD1: Estado actual (un solo registro por center_cd)
# ────────────────────────────────────────────────────────────────
show(
    "SCD TYPE 1 - Estado actual (UUID v7 time-ordered, estable por center_cd)",
    """
    SELECT
        sk_name          AS sk_name_scd1,
        center_cd,
        center_name,
        status,
        _silver_updated_at
    FROM spark_catalog.silver.silver_cost_centers_scd1
    ORDER BY center_cd
    """,
)

# ────────────────────────────────────────────────────────────────
# 2. SCD2: Estado actual (is_current=true)
# ────────────────────────────────────────────────────────────────
show(
    "SCD TYPE 2 - Versiones VIGENTES (UUID v5 deterministico, is_current=true)",
    """
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
    """,
)

# ────────────────────────────────────────────────────────────────
# 3. SCD2: Historial completo (todas las versiones)
# ────────────────────────────────────────────────────────────────
show(
    "SCD TYPE 2 - HISTORIAL COMPLETO (todas las versiones)",
    """
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
    """,
)

# ────────────────────────────────────────────────────────────────
# 4. Comparación Gold
# ────────────────────────────────────────────────────────────────
show(
    "GOLD - Comparacion SCD1 vs SCD2 (side-by-side)",
    """
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
    """,
)

# ────────────────────────────────────────────────────────────────
# 5. Point-in-time query demo (SCD2)
# ────────────────────────────────────────────────────────────────
show(
    "DEMO POINT-IN-TIME: ¿Cuál era el estado de cd004 el 2024-06-01?",
    """
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
    """,
)

# ────────────────────────────────────────────────────────────────
# 6. Iceberg Time Travel (snapshots de bronze)
# ────────────────────────────────────────────────────────────────
show(
    "ICEBERG TIME TRAVEL - Historial de snapshots en bronze",
    """
    SELECT snapshot_id, committed_at, operation, summary
    FROM spark_catalog.bronze.bronze_cost_centers.snapshots
    ORDER BY committed_at
    """,
)

# ────────────────────────────────────────────────────────────────
# 7. Verificar idempotencia UUID v5 en SCD2 (Python puro)
# ────────────────────────────────────────────────────────────────
import uuid
print(f"\n{DIVIDER}")
print("  VERIFICACION IDEMPOTENCIA SCD2 (UUID v5 deterministico)")
print(DIVIDER)
ns = uuid.UUID("6ba7b812-9dad-11d1-80b4-00c04fd430c8")
seed_a = "cd004|2024-03-10 14:00:00"
seed_b = "cd004|2026-03-07 01:31:00"
run1   = str(uuid.uuid5(ns, seed_a))
run2   = str(uuid.uuid5(ns, seed_a))
other  = str(uuid.uuid5(ns, seed_b))
print(f"  seed A  = {seed_a!r}")
print(f"  run1    = {run1}")
print(f"  run2    = {run2}  ← mismo seed")
print(f"  seed B  = {seed_b!r}")
print(f"  other   = {other}  ← versión distinta")
print(f"  run1 == run2   → idempotente {'✅' if run1 == run2 else '❌'}")
print(f"  run1 != other  → distintas versiones = UUIDs distintos {'✅' if run1 != other else '❌'}")

conn.close()

print(f"\n{DIVIDER}")
print("  FIN DE LA DEMOSTRACION")
print(f"  Estrategia: SCD1=uuid7() (Rust) | SCD2=uuid5_sk() (deterministico)")
print(DIVIDER + "\n")
