{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'append',
    file_format          = 'iceberg',
    table_type           = 'iceberg',
    on_schema_change     = 'sync_all_columns',
    post_hook            = "ALTER TABLE {{ this }} SET TBLPROPERTIES ('write.distribution-mode'='range')"
  )
}}

/*
  SILVER — SCD Type 2 | Surrogate Key: UUID v5 deterministico
  ══════════════════════════════════════════════════════════════
  GENERACIÓN DEL SK:
    uuid5_sk(seed) → stdlib Python uuid.uuid5(NAMESPACE, seed)
                     seed = center_cd || '|' || source_updated_at
                     Deterministico e idempotente por diseño

  POR QUÉ UUID v5 Y NO UUID v7 EN SCD2:
    ┌────────────────────────────────────────────────────────────┐
    │  UUID v7 en SCD2 → NO deterministico                      │
    │    Run 1:        cd004|2025-06-01 → "019c8d0f-..."        │
    │    Full-refresh: cd004|2025-06-01 → "019f3a2b-..." ❌     │
    │    → FK en Gold ROTA                                       │
    │                                                            │
    │  UUID v5 en SCD2 → DETERMINISTICO                         │
    │    Run 1:        seed="cd004|2025-06-01" → "a3f4bc91-..." │
    │    Full-refresh: seed="cd004|2025-06-01" → "a3f4bc91-..." │
    │    → FK en Gold INTACTA ✅                                 │
    └────────────────────────────────────────────────────────────┘

  SEED DEL UUID v5:
    concat(center_cd, '|', cast(source_updated_at as string))
    Identifica de forma única una VERSIÓN:
      center_cd        → qué entidad
      source_updated_at → cuándo cambió en SAP

  MECANISMO (2 pasos por run):
    PASO 1 (pre_hook): MERGE INTO cierra versiones vigentes que
                       cambiaron → is_current=false, valid_to=now
    PASO 2 (append):   Inserta nuevas versiones con uuid5_sk(seed)
*/

-- PASO 1: Cerrar versiones activas que cambiaron en SAP
{{ config(
    pre_hook = "
      MERGE INTO {{ this }} AS target
      USING (
          SELECT DISTINCT
              b.center_cd,
              b.source_updated_at AS new_ts
          FROM {{ ref('bronze_cost_centers') }} b
          INNER JOIN {{ this }} t
              ON  b.center_cd         = t.center_cd
              AND t.is_current        = true
              AND b.source_updated_at > t.source_updated_at
      ) AS updates
      ON  target.center_cd  = updates.center_cd
      AND target.is_current = true
      WHEN MATCHED THEN UPDATE SET
          target.is_current = false,
          target.valid_to   = updates.new_ts
    "
) }}

-- PASO 2: Insertar nuevas versiones con UUID v5 deterministico
WITH bronze_data AS (
    SELECT *
    FROM {{ ref('bronze_cost_centers') }}
),

{% if is_incremental() %}
changed_records AS (
    SELECT b.*
    FROM bronze_data b
    LEFT JOIN {{ this }} t
        ON  b.center_cd         = t.center_cd
        AND t.is_current        = true
    WHERE
        t.center_cd IS NULL                           -- nuevo en el datalake
        OR b.source_updated_at > t.source_updated_at  -- cambió en SAP
)
{% else %}
changed_records AS (
    SELECT * FROM bronze_data
)
{% endif %}

SELECT
    /*
      UUID v5 deterministico para esta VERSIÓN del registro.
      Seed = center_cd + '|' + source_updated_at
        → Mismo SAP update = mismo UUID en cualquier run ✅
        → Versiones distintas = UUIDs distintos ✅
        → Full-refresh seguro ✅
    */
    {{ sk_uuid5("concat(center_cd, '|', cast(source_updated_at as string))") }}
                                            AS sk_name,

    center_cd,
    center_name,
    region,
    country,
    cost_type,
    status,
    valid_from,
    source_updated_at,
    source_updated_at                       AS valid_from_sk,
    CAST(NULL AS TIMESTAMP)                 AS valid_to,
    true                                    AS is_current,
    current_timestamp()                     AS _silver_created_at,
    _source_system

FROM changed_records
