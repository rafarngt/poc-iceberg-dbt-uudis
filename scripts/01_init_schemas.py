"""
Script 01 - Inicializar schemas en Spark

Ya no es necesario para el startup — thrift_with_udfs.py
crea los schemas automáticamente al arrancar.

Se mantiene como utilidad para recrear schemas manualmente.

Ejecutar: docker exec spark_thrift_server spark-submit /scripts/01_init_schemas.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("init_schemas").getOrCreate()

for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    print(f"Schema '{schema}' OK")

print("Schemas inicializados.")
spark.stop()
