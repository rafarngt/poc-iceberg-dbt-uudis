{#
  ═══════════════════════════════════════════════════════════════
  Macros de Surrogate Key — Estrategia Híbrida
  ═══════════════════════════════════════════════════════════════

  CONTEXTO:
    El SK (sk_name) se genera en la capa Silver via dos estrategias
    dependiendo del tipo de modelo SCD:

  ┌──────────┬──────────────────┬────────────────────────────────┐
  │ Modelo   │ Macro            │ Por qué                        │
  ├──────────┼──────────────────┼────────────────────────────────┤
  │ SCD1     │ sk_uuid7()       │ UUID v7 time-ordered.          │
  │          │                  │ Generado una vez, preservado   │
  │          │                  │ con COALESCE en incrementales. │
  │          │                  │ Mejor file skipping en Iceberg │
  ├──────────┼──────────────────┼────────────────────────────────┤
  │ SCD2     │ sk_uuid5(seed)   │ UUID v5 deterministico.        │
  │          │                  │ Mismo seed = mismo UUID.       │
  │          │                  │ Seguro en full-refresh.        │
  │          │                  │ FKs downstream nunca se rompen │
  └──────────┴──────────────────┴────────────────────────────────┘

  DEPENDENCIA:
    Ambas UDFs deben estar registradas en la SparkSession antes
    de ejecutar dbt run. Ver: scripts/00_register_udfs.py
#}


{# ─────────────────────────────────────────────────────────────
   sk_uuid7()
   ─────────────────────────────────────────────────────────────
   Genera un UUID v7 time-ordered via uuid-utils (Rust).
   Benchmarked: 3.4M UUIDs/seg — 9.7x más rápido que uuid6.

   Uso (SCD1):
     SELECT {{ sk_uuid7() }} AS sk_name
#}
{% macro sk_uuid7() %}
    uuid7()
{% endmacro %}


{# ─────────────────────────────────────────────────────────────
   sk_uuid5(seed_expr)
   ─────────────────────────────────────────────────────────────
   Genera un UUID v5 deterministico a partir de una expresión
   SQL que actúa como semilla (seed).

   El UUID v5 usa SHA-1 internamente sobre el seed, por lo que:
     - Mismo seed  → mismo UUID (idempotente)
     - Seeds distintos → UUIDs distintos
     - No reversible (one-way hash)

   Uso (SCD2 — seed = clave natural + timestamp de versión):
     SELECT {{ sk_uuid5("concat(center_cd, '|', cast(source_updated_at as string))") }}
            AS sk_name

   Namespace fijo del proyecto:
     '6ba7b812-9dad-11d1-80b4-00c04fd430c8'
     (definido en scripts/00_register_udfs.py — NO cambiar)
#}
{% macro sk_uuid5(seed_expr) %}
    uuid5_sk({{ seed_expr }})
{% endmacro %}
