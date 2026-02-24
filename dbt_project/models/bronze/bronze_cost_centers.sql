{{
  config(
    materialized  = 'incremental',
    incremental_strategy = 'merge',
    unique_key    = 'center_cd',
    file_format   = 'iceberg',
    table_type    = 'iceberg',
    on_schema_change = 'sync_all_columns',
    post_hook     = "ALTER TABLE {{ this }} SET TBLPROPERTIES ('write.format.default'='parquet')"
  )
}}

/*
  BRONZE - Raw ingestion layer
  ────────────────────────────
  - Se ingiere el dato tal cual viene del origen (SAP).
  - Solo se agrega metadata de auditoría (_ingested_at, _source).
  - No hay transformaciones de negocio.
  - Iceberg garantiza ACID: si el registro ya existe (mismo center_cd),
    se hace MERGE (upsert) actualizando todos los campos.
*/

SELECT
    center_cd,
    center_name,
    region,
    country,
    cost_type,
    status,
    CAST(valid_from  AS DATE)      AS valid_from,
    CAST(updated_at  AS TIMESTAMP) AS source_updated_at,
    current_timestamp()            AS _ingested_at,
    'SAP'                          AS _source_system
FROM {{ ref('sap_cost_centers_raw') }}

{% if is_incremental() %}
-- Solo procesa registros más nuevos que el último ingested
WHERE CAST(updated_at AS TIMESTAMP) > (
    SELECT COALESCE(MAX(source_updated_at), CAST('1900-01-01' AS TIMESTAMP))
    FROM {{ this }}
)
{% endif %}
