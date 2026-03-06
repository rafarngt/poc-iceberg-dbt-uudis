# Ejemplos: SCD1 vs SCD2 con Iceberg + dbt

## Datos de ejemplo

### Origen SAP — estado inicial

```
center_cd | center_name   | status   | updated_at
----------+---------------+----------+--------------------
cd004     | Centro Oeste  | INACTIVE | 2024-03-10 14:00:00
```

### Origen SAP — después del UPDATE

```
center_cd | center_name   | status | updated_at
----------+---------------+--------+--------------------
cd004     | Centro Oeste  | ACTIVE | 2025-06-01 09:00:00
```

---

## Diagramas Mermaid

### 1. Flujo general del pipeline

```mermaid
flowchart LR
    SAP["🏭 SAP 
    (origen)"]

    subgraph DL["Datalake — Apache Iceberg + dbt"]
        direction TB
        BRZ["🟫 Bronze 
        bronze_cost_centers
        ─────────────────
        center_cd  ← natural key
        source_updated_at
        _ingested_at
        strategy: MERGE"]

        subgraph SLV["⬜ Silver"]
            SCD1["SCD Type 1
            silver_cost_centers_scd1
            ─────────────────
            sk_name  ← UUID estable
            center_cd
            center_name
            status
            (1 fila por center_cd)"]
            SCD2["SCD Type 2
            silver_cost_centers_scd2
            ─────────────────
            sk_name  ← UUID por versión
            center_cd
            status
            valid_from_sk
            valid_to
            is_current
            (N filas por center_cd)"]
        end

        GLD["🟡 Gold
        gold_cost_centers_comparison
        ─────────────────
        sk1_uuid vs sk2_uuid
        has_history
        total_versions"]
    end

    DS["📊 Downstream
    (BI, ML, APIs)"]

    SAP -->|"dbt seed / ingest"| BRZ
    BRZ -->|"incremental MERGE
    uniquekey=center_cd"| SCD1
    BRZ -->|"pre_hook MERGE +
    append new version"| SCD2
    SCD1 --> GLD
    SCD2 --> GLD
    GLD --> DS
```

---

### 2. SCD Type 1 — comportamiento ante un UPDATE

```mermaid
sequenceDiagram
    participant SAP as 🏭 SAP
    participant BRZ as Bronze
    participant SCD1 as Silver SCD1
    participant DS as Downstream

    Note over SAP,DS: ── PRIMERA CARGA ──────────────────────────────────

    SAP->>BRZ: INSERT cd004 | INACTIVE | 2024-03-10
    BRZ->>SCD1: dbt run (full)
    Note over SCD1: uuid() → "aaa-111"<br/>sk_name="aaa-111" | cd004 | INACTIVE

    SCD1-->>DS: FK: sk_name = "aaa-111"

    Note over SAP,DS: ── SAP MANDA UPDATE ───────────────────────────────

    SAP->>BRZ: MERGE cd004 | ACTIVE | 2025-06-01
    Note over BRZ: Iceberg MERGE actualiza<br/>el registro cd004

    BRZ->>SCD1: dbt run (incremental)
    Note over SCD1: COALESCE(existing.sk_name, uuid())<br/>= "aaa-111"  ← mismo UUID!<br/>status sobreescrito → ACTIVE

    SCD1-->>DS: FK: sk_name = "aaa-111" ✅ intacta
    Note over DS: Downstream NO se entera del cambio<br/>Su FK sigue funcionando igual
```

---

### 3. SCD Type 2 — comportamiento ante un UPDATE

```mermaid
sequenceDiagram
    participant SAP as 🏭 SAP
    participant BRZ as Bronze
    participant SCD2 as Silver SCD2
    participant DS as Downstream

    Note over SAP,DS: ── PRIMERA CARGA ──────────────────────────────────

    SAP->>BRZ: INSERT cd004 | INACTIVE | 2024-03-10
    BRZ->>SCD2: dbt run (full)
    Note over SCD2: uuid() → "aaa-111"<br/>sk="aaa-111" | INACTIVE | is_current=true<br/>valid_from=2024-03-10 | valid_to=NULL

    SCD2-->>DS: usa sk_name = "aaa-111"

    Note over SAP,DS: ── SAP MANDA UPDATE ───────────────────────────────

    SAP->>BRZ: MERGE cd004 | ACTIVE | 2025-06-01
    BRZ->>SCD2: dbt run (incremental)

    Note over SCD2: pre_hook — Iceberg MERGE INTO:<br/>UPDATE sk="aaa-111"<br/>SET is_current=false<br/>    valid_to=2025-06-01 09:00:00

    Note over SCD2: APPEND nuevo registro:<br/>uuid() → "bbb-222"<br/>sk="bbb-222" | ACTIVE | is_current=true<br/>valid_from=2025-06-01 | valid_to=NULL

    SCD2-->>DS: sk_name vigente = "bbb-222"
    Note over DS: Downstream que usaba "aaa-111"<br/>puede seguir consultando el estado HISTÓRICO<br/>Downstream nuevo usa "bbb-222"
```

---

### 4. Estado de la tabla SCD2 tras el UPDATE

```mermaid
block-beta
  columns 5
  H1["sk_name"]:1 H2["center_cd"]:1 H3["status"]:1 H4["is_current"]:1 H5["valid_to"]:1
  R1A["aaa-111"]:1 R1B["cd004"]:1 R1C["INACTIVE"]:1 R1D["false ❌"]:1 R1E["2025-06-01"]:1
  R2A["bbb-222"]:1 R2B["cd004"]:1 R2C["ACTIVE"]:1 R2D["true ✅"]:1 R2E["NULL"]:1
```

---

### 5. Iceberg MERGE INTO — el mecanismo interno

```mermaid
flowchart TD
    A["Bronze
    cd004 | ACTIVE | 2025-06-01"] --> B{{"¿Existe cd004
    en SCD2
    con is_current=true?"}}

    B -- SÍ --> C["Iceberg MERGE INTO
    ─────────────────
    WHEN MATCHED THEN UPDATE
      is_current = false 
      valid_to = 2025-06-01"]
    B -- NO --> D["Registro nuevo
    (sin historial previo)"]

    C --> E["APPEND nuevo registro
    ─────────────────
    sk_name    = uuid()  → 'bbb-222'
    center_cd  = 'cd004'
    status     = 'ACTIVE'
    is_current = true
    valid_from = 2025-06-01
    valid_to   = NULL"]
    D --> E

    E --> F[("Silver SCD2
    (tabla Iceberg)
    ────────────────
    aaa-111 | INACTIVE | false ❌
    bbb-222 | ACTIVE   | true  ✅")]
```

---

## Código core

### Bronze — `bronze_cost_centers.sql`

El único trabajo de bronze es **MERGE con upsert** sobre la clave natural:
dbt traduce `incremental_strategy = 'merge'` a un `MERGE INTO` de Iceberg.

```sql
-- dbt config → Iceberg MERGE INTO (upsert por center_cd)
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'center_cd',      -- ← clave del MERGE
    file_format          = 'iceberg'
) }}

SELECT
    center_cd,
    center_name,
    status,
    CAST(updated_at AS TIMESTAMP) AS source_updated_at,
    current_timestamp()           AS _ingested_at,
    'SAP'                         AS _source_system
FROM {{ ref('sap_cost_centers_raw') }}

{% if is_incremental() %}
-- Watermark: solo registros más nuevos que el último procesado
WHERE CAST(updated_at AS TIMESTAMP) > (
    SELECT COALESCE(MAX(source_updated_at), CAST('1900-01-01' AS TIMESTAMP))
    FROM {{ this }}
)
{% endif %}
```

**SQL que genera Iceberg internamente:**

```sql
MERGE INTO local.bronze.bronze_cost_centers AS target
USING (
    SELECT 'cd004' AS center_cd, 'ACTIVE' AS status,
           TIMESTAMP'2025-06-01 09:00:00' AS source_updated_at, ...
) AS source
ON target.center_cd = source.center_cd          -- join por clave natural
WHEN MATCHED THEN UPDATE SET                     -- actualiza si ya existe
    target.status           = source.status,
    target.source_updated_at = source.source_updated_at, ...
WHEN NOT MATCHED THEN INSERT (...)               -- inserta si es nuevo
VALUES (...)
```

---

### Silver SCD1 — el truco del `COALESCE`

El núcleo de SCD1 es esta lógica: **devuelve el UUID existente si el
registro ya existía, o genera uno nuevo si es la primera vez.**

```sql
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = 'center_cd',   -- ← mismo MERGE, un registro por cd
    file_format          = 'iceberg'
) }}

-- En modo incremental: recuperar UUIDs ya asignados
{% if is_incremental() %}
existing_keys AS (
    SELECT center_cd, sk_name FROM {{ this }}
),
{% endif %}

-- Solo procesar lo que cambió en bronze
enriched AS (
    SELECT * FROM {{ ref('bronze_cost_centers') }}
    {% if is_incremental() %}
    WHERE source_updated_at > (SELECT MAX(source_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- ↓ NÚCLEO DE SCD1: preservar UUID si ya existe, generar si es nuevo
    {% if is_incremental() %}
    COALESCE(ek.sk_name, uuid()) AS sk_name,
    {% else %}
    uuid()                        AS sk_name,
    {% endif %}
    e.center_cd,
    e.center_name,
    e.status,
    ...
FROM enriched e
{% if is_incremental() %}
LEFT JOIN existing_keys ek ON e.center_cd = ek.center_cd
{% endif %}
```

**Resultado para cd004 antes y después del UPDATE:**

```
-- ANTES del update SAP:
sk_name    | center_cd | status
-----------+-----------+---------
"aaa-111"  | cd004     | INACTIVE

-- DESPUÉS del update SAP (dbt run incremental):
-- COALESCE("aaa-111", uuid()) = "aaa-111"  ← UUID no cambia
sk_name    | center_cd | status
-----------+-----------+---------
"aaa-111"  | cd004     | ACTIVE   ← solo el dato cambió
```

---

### Silver SCD2 — el `pre_hook` + `append`

SCD2 requiere **dos operaciones atómicas**: cerrar la versión vieja
y abrir la nueva. Se implementa con `pre_hook` (Iceberg MERGE) + `append`.

```sql
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'append',    -- ← solo inserta, nunca actualiza
    file_format          = 'iceberg',

    -- PASO 1: cerrar versiones activas que cambiaron (pre_hook)
    pre_hook = "
        MERGE INTO {{ this }} AS target
        USING (
            -- Detectar: registros que existen en bronze con timestamp más nuevo
            SELECT DISTINCT b.center_cd, b.source_updated_at
            FROM {{ ref('bronze_cost_centers') }} b
            INNER JOIN {{ this }} t
                ON  b.center_cd        = t.center_cd
                AND t.is_current       = true
                AND b.source_updated_at > t.source_updated_at
        ) AS updates
        ON  target.center_cd  = updates.center_cd
        AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.is_current = false,                       -- ← cerrar versión
            target.valid_to   = updates.source_updated_at   -- ← sellar timestamp
    "
) }}

-- PASO 2: insertar nueva versión con nuevo UUID (append)
WITH changed_records AS (
    SELECT b.*
    FROM {{ ref('bronze_cost_centers') }} b
    LEFT JOIN {{ this }} t
        ON  b.center_cd = t.center_cd AND t.is_current = true
    WHERE t.center_cd IS NULL                      -- nuevo en datalake
       OR b.source_updated_at > t.source_updated_at -- cambió en SAP
)

SELECT
    uuid()                  AS sk_name,        -- ← NUEVO UUID por versión
    center_cd,
    center_name,
    status,
    source_updated_at       AS valid_from_sk,  -- inicio de vigencia
    CAST(NULL AS TIMESTAMP) AS valid_to,       -- abierto
    true                    AS is_current,
    ...
FROM changed_records
```

**Resultado para cd004 antes y después del UPDATE:**

```
-- ANTES del update SAP:
sk_name    | center_cd | status   | is_current | valid_to
-----------+-----------+----------+------------+----------
"aaa-111"  | cd004     | INACTIVE | true       | NULL

-- DESPUÉS del update SAP:

-- pre_hook cerró la versión vieja:
sk_name    | center_cd | status   | is_current | valid_to
-----------+-----------+----------+------------+--------------------
"aaa-111"  | cd004     | INACTIVE | false      | 2025-06-01 09:00:00

-- append insertó la versión nueva:
sk_name    | center_cd | status   | is_current | valid_to
-----------+-----------+----------+------------+---------
"bbb-222"  | cd004     | ACTIVE   | true       | NULL
```

---

### Gold — Comparación side-by-side

```sql
-- SCD1: siempre 1 fila, UUID estable
SELECT sk_name AS sk1_uuid, center_cd, status
FROM silver_cost_centers_scd1
-- → "aaa-111" | cd004 | ACTIVE   (mismo UUID de siempre)

-- SCD2: versión vigente
SELECT sk_name AS sk2_uuid_current, center_cd, status
FROM silver_cost_centers_scd2
WHERE is_current = true
-- → "bbb-222" | cd004 | ACTIVE   (UUID nuevo)

-- Point-in-time: ¿qué tenía cd004 antes del update?
SELECT sk_name, status, valid_from_sk, valid_to
FROM silver_cost_centers_scd2
WHERE center_cd = 'cd004'
  AND valid_from_sk <= TIMESTAMP'2024-12-31'
  AND (valid_to IS NULL OR valid_to > TIMESTAMP'2024-12-31')
-- → "aaa-111" | INACTIVE | 2024-03-10 | 2025-06-01
```

---

## Resumen de decisión

```mermaid
flowchart TD
    Q1{"¿El downstream necesita\nhistorial o auditoría?"}

    Q1 -- NO --> SCD1["✅ SCD Type 1\n─────────────\nsk_name estable\n1 fila por center_cd\ndbt: merge + COALESCE\nMás simple, más rápido"]
    Q1 -- SÍ --> Q2{"¿Necesita saber\nel estado en\nun punto del tiempo?"}

    Q2 -- SÍ --> SCD2["✅ SCD Type 2\n─────────────\nsk_name por versión\nN filas por center_cd\ndbt: pre_hook MERGE + append\nPoint-in-time queries"]
    Q2 -- NO --> SCD1

    SCD1 --> NOTE1["⚠️ Si el dato cambia en SAP:\nel SK no cambia\nlos datos se sobreescriben\nno hay forma de recuperar el estado anterior"]
    SCD2 --> NOTE2["⚠️ Si el dato cambia en SAP:\nse genera nuevo SK\nel SK anterior queda cerrado con valid_to\nel downstream upstream puede seguir usando el SK viejo"]
```
