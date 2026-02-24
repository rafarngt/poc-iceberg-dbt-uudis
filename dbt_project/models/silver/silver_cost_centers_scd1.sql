{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'center_cd',
    file_format          = 'iceberg',
    table_type           = 'iceberg',
    on_schema_change     = 'sync_all_columns',
    post_hook            = "ALTER TABLE {{ this }} SET TBLPROPERTIES ('write.distribution-mode'='range')"
  )
}}

/*
  SILVER — SCD Type 1 | Surrogate Key: UUID v7 estable
  ═══════════════════════════════════════════════════════
  GENERACIÓN DEL SK:
    uuid7()  → uuid-utils (Rust, 9.7x más rápido que uuid6)
               3.4M UUIDs/seg | 0.29 µs/uuid
               RFC 9562 compliant | time-ordered

  ESTRATEGIA:
    ┌─────────────────────────────────────────────────────┐
    │ Primera carga   → uuid7() genera SK para cada fila  │
    │ Run incremental → COALESCE(existing_sk, uuid7())    │
    │                   Si el registro ya existe: mismo SK│
    │                   Si es nuevo: nuevo uuid7()        │
    └─────────────────────────────────────────────────────┘

  VENTAJAS UUID v7 vs UUID v4 en Iceberg:
    → Time-ordered: archivos Parquet con rangos min/max acotados
    → Iceberg file skipping activo (UUID v4 lo destruye)
    → Mejor compresión por prefix sharing del timestamp
    → Ordenable: ORDER BY sk_name ≈ ORDER BY _silver_updated_at

  ¿QUÉ PASA SI SAP ACTUALIZA UN REGISTRO?
    → El sk_name NO cambia (COALESCE preserva el SK original)
    → Los datos de negocio se sobreescriben
    → Las FKs en downstream se mantienen intactas
    → El historial NO se conserva (eso es SCD1 por diseño)
*/

WITH bronze_data AS (
    SELECT *
    FROM {{ ref('bronze_cost_centers') }}
),

{% if is_incremental() %}
existing_keys AS (
    SELECT center_cd, sk_name
    FROM {{ this }}
),
{% endif %}

changed AS (
    SELECT
        b.center_cd,
        b.center_name,
        b.region,
        b.country,
        b.cost_type,
        b.status,
        b.valid_from,
        b.source_updated_at,
        b._source_system
    FROM bronze_data b

    {% if is_incremental() %}
    WHERE b.source_updated_at > (
        SELECT COALESCE(MAX(source_updated_at), CAST('1900-01-01' AS TIMESTAMP))
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    -- SCD1: preservar UUID v7 existente O generar uno nuevo (uuid-utils Rust)
    {% if is_incremental() %}
    COALESCE(ek.sk_name, {{ sk_uuid7() }})  AS sk_name,
    {% else %}
    {{ sk_uuid7() }}                         AS sk_name,
    {% endif %}

    c.center_cd,
    c.center_name,
    c.region,
    c.country,
    c.cost_type,
    c.status,
    c.valid_from,
    c.source_updated_at,
    current_timestamp()                      AS _silver_updated_at,
    c._source_system

FROM changed c

{% if is_incremental() %}
LEFT JOIN existing_keys ek ON c.center_cd = ek.center_cd
{% endif %}
