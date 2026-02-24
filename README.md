# Spark 3.5.3 + Iceberg 1.6.1 + dbt — PoC Surrogate Keys (SCD1 vs SCD2)

Stack: **Apache Spark 3.5.3 · Apache Iceberg 1.6.1 · dbt-spark 1.8 · uuid-utils 0.14.1 · Docker**

## El problema que resuelve esta PoC

```
SAP (origen)  →  Bronze (raw)  →  Silver (SK generado)  →  Gold  →  downstream
```

En Silver se genera un **Surrogate Key (sk_name)** para cada registro de SAP.
La pregunta clave es:

> **¿Qué pasa si un registro se actualiza en SAP?
> ¿Cómo llevo ese UPDATE manteniendo la integridad del sk_name?**

Esta PoC implementa y compara dos estrategias SCD con una **estrategia híbrida de generación de SKs**.

---

## Estrategia híbrida de Surrogate Keys

```
┌──────────┬──────────────────────┬─────────────────────────────────────────────┐
│ Modelo   │ Función SQL          │ Por qué                                     │
├──────────┼──────────────────────┼─────────────────────────────────────────────┤
│ SCD1     │ uuid7()              │ UUID v7 time-ordered. Generado UNA VEZ      │
│          │ (uuid-utils, Rust)   │ y preservado con COALESCE. Mejor file       │
│          │ 3.4M UUIDs/seg       │ skipping en Iceberg que UUID v4.            │
├──────────┼──────────────────────┼─────────────────────────────────────────────┤
│ SCD2     │ uuid5_sk(seed)       │ UUID v5 deterministico. Mismo seed =        │
│          │ (stdlib Python)      │ mismo UUID SIEMPRE. Seguro en full-refresh. │
│          │                      │ seed = center_cd + '|' + source_updated_at  │
└──────────┴──────────────────────┴─────────────────────────────────────────────┘
```

### ¿Por qué UUID v7 para SCD1?

| Escenario | Comportamiento |
|-----------|---------------|
| Primera carga | `uuid7()` genera SK para `cd004` → `019c8d0f-...` (time-ordered) |
| SAP actualiza `cd004` (INACTIVE → ACTIVE) | SK **sigue siendo** `019c8d0f-...` (COALESCE lo preserva) |
| Downstream | Su FK `sk_name = 019c8d0f-...` nunca se rompe |
| Ventaja vs UUID v4 | Iceberg file skipping activo por prefijo temporal |

### ¿Por qué UUID v5 para SCD2?

| Escenario | UUID v7 ❌ | UUID v5 ✅ |
|-----------|-----------|-----------|
| Run 1 para cd004 v1 | `019c8d0f-...` | `a3f4bc91-...` |
| Full-refresh del modelo | `019f3a2b-...` ← **DIFERENTE** | `a3f4bc91-...` ← **IGUAL** |
| FK en Gold tras full-refresh | 💥 ROTA | ✅ INTACTA |

El seed `center_cd + source_updated_at` identifica de forma única una **versión** de un registro, haciéndolo reproducible en cualquier run.

---

## Estructura del proyecto

```
.
├── Dockerfile                        # Spark 3.5.3 + Iceberg 1.6.1 + dbt + uuid-utils
├── docker-compose.yml                # spark-thrift (:10000) + dbt runner
├── spark_conf/
│   └── spark-defaults.conf           # Iceberg catalog config
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # dbt → Spark Thrift Server
│   ├── macros/
│   │   ├── generate_schema_name.sql  # Schema sin prefijo dev_
│   │   └── generate_sk.sql           # sk_uuid7() y sk_uuid5(seed)
│   ├── seeds/
│   │   └── sap_cost_centers_raw.csv  # 5 centros de costo SAP
│   └── models/
│       ├── bronze/
│       │   └── bronze_cost_centers.sql       # MERGE por center_cd
│       ├── silver/
│       │   ├── silver_cost_centers_scd1.sql  # SK: uuid7() estable
│       │   └── silver_cost_centers_scd2.sql  # SK: uuid5_sk(seed) por versión
│       └── gold/
│           ├── gold_cost_centers_comparison.sql    # SCD1 vs SCD2 side-by-side
│           └── gold_cost_centers_scd2_history.sql  # Historial point-in-time
└── scripts/
    ├── 00_register_udfs.py      # Registrar uuid7() y uuid5_sk() en Spark
    ├── 01_init_schemas.py       # Namespaces Iceberg + registro UDFs
    ├── 02_simulate_sap_update.py # Simular UPDATE en SAP
    └── 03_query_comparison.py   # Ver diferencias SCD1 vs SCD2
```

---

## Cómo ejecutar

### 1. Build y arranque

```bash
docker compose build
docker compose up -d

# Esperar a que spark-thrift esté healthy (~30s)
docker compose ps
```

### 2. Inicializar namespaces + registrar UDFs

```bash
# Crea namespaces Iceberg Y registra uuid7() / uuid5_sk() en Spark
docker exec spark_thrift_server spark-submit /scripts/01_init_schemas.py
```

### 3. Cargar datos iniciales y correr modelos dbt

```bash
docker exec -it dbt_runner bash

# Dentro del contenedor:
dbt seed    # Carga sap_cost_centers_raw.csv → local.raw
dbt run     # bronze → silver SCD1 (uuid7) → silver SCD2 (uuid5) → gold
dbt test    # Valida unique/not_null
```

### 4. Ver estado inicial

```bash
docker exec spark_thrift_server spark-submit /scripts/03_query_comparison.py
```

### 5. Simular UPDATE en SAP

```bash
# cd004: INACTIVE → ACTIVE  |  cd003: rename  |  cd006: nuevo registro
docker exec spark_thrift_server spark-submit /scripts/02_simulate_sap_update.py
```

### 6. Re-ejecutar dbt

```bash
docker exec -it dbt_runner bash
dbt run
```

### 7. Comparar resultados

```bash
docker exec spark_thrift_server spark-submit /scripts/03_query_comparison.py
```

**Resultado esperado para `cd004` tras el UPDATE en SAP:**

| Tabla | sk_name | status | Comportamiento |
|-------|---------|--------|----------------|
| SCD1 | `019c8d0f-...` (igual) | ACTIVE | UUID v7 preservado por COALESCE |
| SCD2 vigente | `d7e2ac15-...` (nuevo) | ACTIVE | UUID v5 de la nueva versión |
| SCD2 histórico | `a3f4bc91-...` (cerrado) | INACTIVE | UUID v5 de la versión anterior, `valid_to` sellado |

---

## Iceberg Time Travel

```sql
-- Ver snapshots de bronze
SELECT * FROM local.bronze.bronze_cost_centers.snapshots;

-- Estado de bronze ANTES del update
SELECT * FROM local.bronze.bronze_cost_centers
VERSION AS OF <snapshot_id>;
```

---

## Tecnologías

| Componente | Versión | Rol |
|-----------|---------|-----|
| Apache Spark | 3.5.3 | Motor de procesamiento |
| Apache Iceberg | 1.6.1 | Table format ACID |
| dbt-spark | 1.8.0 | Transformaciones |
| dbt-core | 1.8.0 | Orquestación dbt |
| uuid-utils | 0.14.1 | UUID v7 Rust (SCD1 SK) |
| OpenJDK | 17 | JVM para Spark |
| Python | 3.9+ | Runtime |
| Scala | 2.12 | Spark compiled |
| Hadoop | 3 | Storage layer |
