{{
  config(
    materialized = 'table',
    file_format  = 'iceberg',
    table_type   = 'iceberg'
  )
}}

/*
  GOLD - Comparación SCD1 vs SCD2
  ────────────────────────────────
  Vista analítica que muestra side-by-side:
    - El sk_name asignado en SCD1 (estable, uno por center_cd)
    - El sk_name actual asignado en SCD2 (uno por versión vigente)
    - La cantidad de versiones históricas que tiene cada center_cd en SCD2
    - Si hubo actualizaciones (has_history = true cuando SCD2 tiene > 1 versión)

  Útil para demostrar la diferencia de comportamiento ante un UPDATE del origen.
*/

WITH scd1_current AS (
    SELECT
        sk_name          AS sk1_stable,
        center_cd,
        center_name,
        status,
        cost_type,
        source_updated_at AS scd1_last_update,
        _silver_updated_at
    FROM {{ ref('silver_cost_centers_scd1') }}
),

scd2_current AS (
    SELECT
        sk_name          AS sk2_current,
        center_cd,
        center_name,
        status,
        valid_from_sk,
        valid_to,
        is_current,
        source_updated_at AS scd2_last_update
    FROM {{ ref('silver_cost_centers_scd2') }}
    WHERE is_current = true
),

scd2_history_count AS (
    SELECT
        center_cd,
        COUNT(*)          AS total_versions,
        MIN(valid_from_sk) AS first_version_ts,
        MAX(valid_from_sk) AS last_version_ts
    FROM {{ ref('silver_cost_centers_scd2') }}
    GROUP BY center_cd
)

SELECT
    -- Identificadores
    s1.center_cd,

    -- SCD1: SK estable
    s1.sk1_stable                                    AS sk1_uuid,
    s1.center_name                                   AS scd1_center_name,
    s1.status                                        AS scd1_status,
    s1.scd1_last_update,

    -- SCD2: SK de la versión vigente
    s2.sk2_current                                   AS sk2_uuid_current,
    s2.center_name                                   AS scd2_center_name,
    s2.status                                        AS scd2_status,
    s2.scd2_last_update,
    s2.valid_from_sk                                 AS scd2_valid_from,

    -- Historial SCD2
    h.total_versions                                 AS scd2_total_versions,
    h.first_version_ts                               AS scd2_first_version,
    h.last_version_ts                                AS scd2_latest_version,
    CASE WHEN h.total_versions > 1 THEN true
         ELSE false END                              AS has_history,

    -- ¿Los SK son iguales? (solo lo serían si uuid() generara mismo valor, lo cual es imposible)
    -- Esta columna muestra conceptualmente que son independientes
    'SCD1_SK_IS_STABLE'                              AS scd1_behavior,
    'SCD2_SK_CHANGES_ON_UPDATE'                      AS scd2_behavior,

    current_timestamp()                              AS _gold_created_at

FROM scd1_current s1
LEFT JOIN scd2_current s2
    ON s1.center_cd = s2.center_cd
LEFT JOIN scd2_history_count h
    ON s1.center_cd = h.center_cd
