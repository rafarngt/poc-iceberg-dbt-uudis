{{
  config(
    materialized = 'table',
    file_format  = 'iceberg',
    table_type   = 'iceberg'
  )
}}

/*
  GOLD - Historial completo SCD2 (point-in-time ready)
  ──────────────────────────────────────────────────────
  Expone el historial completo de versiones de cada centro de costo.
  Permite consultas de tipo: "¿cuál era el estado de cd004 el 2024-02-01?"
*/

SELECT
    sk_name,
    center_cd,
    center_name,
    region,
    country,
    cost_type,
    status,
    valid_from,
    source_updated_at,
    valid_from_sk,
    valid_to,
    is_current,
    CASE
        WHEN is_current THEN 'VIGENTE'
        ELSE 'HISTORICO'
    END                       AS record_state,
    -- Duración del periodo (NULL si is_current=true, aún vigente)
    CASE
        WHEN valid_to IS NOT NULL
        THEN CAST((unix_timestamp(valid_to) - unix_timestamp(valid_from_sk)) / 86400 AS INT)
        ELSE NULL
    END                       AS days_valid,
    _silver_created_at,
    current_timestamp()       AS _gold_created_at
FROM {{ ref('silver_cost_centers_scd2') }}
ORDER BY center_cd, valid_from_sk
