"""
Script 02 - Simular actualizaciones en el origen SAP
─────────────────────────────────────────────────────
Simula un UPDATE que llega desde SAP actualizando la capa RAW
via conexión directa al Thrift Server (mismo contexto que dbt).

Esto garantiza que el pipeline completo funcione correctamente:
  raw → bronze → silver (SCD1 + SCD2) → gold

Cambios simulados:
  - cd004: INACTIVE → ACTIVE  (cambio de status)
  - cd003: rename → "Centro Este 2.0"
  - cd006: nuevo registro "Centro Bajio"

Ejecutar:
  python3 /scripts/02_simulate_sap_update.py
  (desde spark_thrift_server o dbt_runner)
"""

from pyhive import hive
from datetime import datetime

NOW  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
HOST = "spark-thrift"

conn = hive.connect(HOST, port=10000, auth="NOSASL")
cur  = conn.cursor()

print(f"\n{'='*60}")
print(f"  SIMULACION DE UPDATE EN SAP - {NOW}")
print(f"{'='*60}\n")

# ─── Estado actual (bronze, antes del update) ───────────────
print("Estado ANTES del update en bronze:")
cur.execute("""
    SELECT center_cd, center_name, status, source_updated_at
    FROM spark_catalog.bronze.bronze_cost_centers
    ORDER BY center_cd
""")
rows = cur.fetchall()
print(f"{'center_cd':<12} {'center_name':<20} {'status':<10} source_updated_at")
print("-" * 65)
for r in rows:
    print(f"{r[0]:<12} {r[1]:<20} {r[2]:<10} {r[3]}")

# ─── Actualizar raw via dos pasos para evitar auto-referencia ────
# INSERT OVERWRITE leyendo de la misma tabla falla con Spark
# (UNSUPPORTED_OVERWRITE.TABLE). Solución: tabla temporal →
# INSERT OVERWRITE desde temp → DROP temp.

# Paso 1: materializar los datos modificados en una tabla temporal
cur.execute(f"""
    CREATE TABLE raw.sap_raw_tmp AS
    SELECT
        center_cd,
        CASE WHEN center_cd = 'cd003' THEN 'Centro Este 2.0' ELSE center_name END AS center_name,
        region,
        country,
        cost_type,
        CASE WHEN center_cd = 'cd004' THEN 'ACTIVE' ELSE status END AS status,
        valid_from,
        CASE
            WHEN center_cd IN ('cd003', 'cd004') THEN TIMESTAMP '{NOW}'
            ELSE updated_at
        END AS updated_at
    FROM raw.sap_cost_centers_raw
    UNION ALL
    SELECT 'cd006', 'Centro Bajio', 'Bajio', 'MX', 'OPEX', 'ACTIVE',
           DATE '2025-01-01', TIMESTAMP '{NOW}'
""")

# Paso 2: sobreescribir raw desde la temporal (sin auto-referencia)
cur.execute("INSERT OVERWRITE raw.sap_cost_centers_raw SELECT * FROM raw.sap_raw_tmp")

# Paso 3: limpiar tabla temporal
cur.execute("DROP TABLE raw.sap_raw_tmp")

print("\nRaw actualizado con cambios de SAP:")
cur.execute("""
    SELECT center_cd, center_name, status, updated_at
    FROM raw.sap_cost_centers_raw
    ORDER BY center_cd
""")
rows = cur.fetchall()
print(f"{'center_cd':<12} {'center_name':<20} {'status':<10} updated_at")
print("-" * 65)
for r in rows:
    print(f"{r[0]:<12} {r[1]:<20} {r[2]:<10} {r[3]}")

conn.close()

print("\n" + "="*60)
print("  AHORA ejecuta: dbt run  (desde el contenedor dbt_runner)")
print("  Pipeline: raw -> bronze -> silver (SCD1+SCD2) -> gold")
print("  Luego ejecuta: spark-submit /scripts/03_query_comparison.py")
print("="*60 + "\n")
