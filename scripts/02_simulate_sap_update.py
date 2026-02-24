"""
Script 02 - Simular actualizaciones en el origen SAP
─────────────────────────────────────────────────────
Este script simula un UPDATE que llega desde SAP:
  - Actualiza el status de cd004 de INACTIVE -> ACTIVE
  - Cambia el center_name de cd003
  - Agrega un nuevo registro cd006

Después de correr este script, vuelve a ejecutar dbt run
y observa la diferencia de comportamiento entre SCD1 y SCD2.

Ejecutar:
  spark-submit /scripts/02_simulate_sap_update.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("simulate_sap_update")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hadoop")
    .config("spark.sql.catalog.iceberg.warehouse", "/opt/data/warehouse")
    .config("spark.sql.defaultCatalog", "iceberg")
    .getOrCreate()
)

NOW = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

print(f"\n{'='*60}")
print(f"  SIMULACION DE UPDATE EN SAP - {NOW}")
print(f"{'='*60}\n")

# ─── Leer tabla bronze actual ───────────────────────────────
print("Estado ANTES del update en bronze:")
spark.sql("SELECT center_cd, center_name, status, source_updated_at FROM iceberg.bronze.bronze_cost_centers").show()

# ─── MERGE bronze: simular updates SAP ──────────────────────
spark.sql(f"""
    MERGE INTO iceberg.bronze.bronze_cost_centers AS target
    USING (
        SELECT * FROM VALUES
            -- cd004: INACTIVE -> ACTIVE  (update de status)
            ('cd004', 'Centro Oeste',    'Oeste', 'MX', 'CAPEX', 'ACTIVE',   DATE'2023-06-01', TIMESTAMP'{NOW}', TIMESTAMP'{NOW}', 'SAP'),
            -- cd003: cambio de nombre
            ('cd003', 'Centro Este 2.0', 'Este',  'MX', 'OPEX',  'ACTIVE',   DATE'2023-03-01', TIMESTAMP'{NOW}', TIMESTAMP'{NOW}', 'SAP'),
            -- cd006: registro nuevo
            ('cd006', 'Centro Bajio',    'Bajio', 'MX', 'OPEX',  'ACTIVE',   DATE'2025-01-01', TIMESTAMP'{NOW}', TIMESTAMP'{NOW}', 'SAP')
        AS updates(center_cd, center_name, region, country, cost_type, status, valid_from, source_updated_at, _ingested_at, _source_system)
    ) AS src
    ON target.center_cd = src.center_cd
    WHEN MATCHED THEN UPDATE SET
        target.center_name       = src.center_name,
        target.status            = src.status,
        target.source_updated_at = src.source_updated_at,
        target._ingested_at      = src._ingested_at
    WHEN NOT MATCHED THEN INSERT *
""")

print("\nEstado DESPUES del update en bronze:")
spark.sql("SELECT center_cd, center_name, status, source_updated_at FROM iceberg.bronze.bronze_cost_centers ORDER BY center_cd").show()

print("\n" + "="*60)
print("  AHORA ejecuta: dbt run  (desde el contenedor dbt_runner)")
print("  Luego ejecuta: python3 /scripts/03_query_comparison.py")
print("="*60 + "\n")

spark.stop()
