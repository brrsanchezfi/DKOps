# Arquitectura

## Visión general

DKOps implementa la **arquitectura Medallion** (Landing → Bronze → Silver → Gold) con un motor de orquestación declarativo que separa la configuración del comportamiento.

```mermaid
flowchart TB
    subgraph src["Fuentes externas"]
        F1["JSON / CSV\nParquet / Avro"]
        F2["Kafka / Event Hub\nStreaming"]
    end

    subgraph landing["☁ Landing Zone"]
        L["Archivos crudos\nDepositados por Data Factory\nKafka Connector, FTP, etc."]
    end

    subgraph bronze["🥉 Bronze — Raw + metadata"]
        B1["ventas_raw\n_ingested_at · _ingested_date\n_source_file"]
        B2["clientes_raw\nop_type: I/U/D"]
        B3["eventos_raw\nStreaming → Batch"]
    end

    subgraph silver["🥈 Silver — Estado actual"]
        S1["ventas_current\ncdc_merge"]
        S2["clientes_current\nfull_merge"]
        S3["eventos_current\nappend_dedup"]
    end

    subgraph gold["🥇 Gold — KPIs y agregados"]
        G1["revenue_diario"]
        G2["engagement_clientes"]
        G3["alertas_criticas"]
    end

    F1 --> L
    F2 --> L
    L -->|"ingest_bronze()\nPartition overwrite idempotente"| B1
    L -->|"run_streaming()\navailableNow"| B3
    L --> B2
    B1 -->|"cdc_merge"| S1
    B2 -->|"full_merge"| S2
    B3 -->|"append_dedup"| S3
    S1 & S2 & S3 -->|"TableWriter SQL"| G1 & G2 & G3
```

---

## Módulos del framework

### Módulo 1: `ingestion` (Landing → Silver)

```mermaid
flowchart LR
    subgraph contracts["Contratos JSON"]
        BC["ingestion/batch/\n*.json"]
        SC["ingestion/streaming/\n*.json"]
        SV["ingestion/silver/\n*.json"]
    end

    subgraph engine["IngestionEngine"]
        IE["from_spark()"]
        BI["BronzeIngestor\ningest_bronze()"]
        SS["run_streaming()\navailableNow"]
        SP["SilverPromoter\npromote_silver()"]
    end

    subgraph strategies["Estrategias Silver"]
        FM["full_merge\nMERGE INTO\nSCD1 completo"]
        CM["cdc_merge\nI/U/D + soft delete\nis_deleted"]
        IR["incremental_replace\nReemplaza por watermark"]
        AD["append_dedup\nAnti-join append"]
    end

    BC --> BI
    SC --> SS
    SV --> SP
    IE --> BI & SS & SP
    SP --> FM & CM & IR & AD
```

#### BronzeIngestor — Landing → Bronze

1. Lee archivos del directorio `source.path` del contrato
2. Añade metadatos: `_ingested_at`, `_ingested_date`, `_source_file`
3. Escribe con **partition overwrite** por `_ingested_date` → idempotente

#### SilverPromoter — Bronze → Silver

1. Lee Bronze completo (o filtrado)
2. Deduplica por `merge_keys` según `watermark_col`
3. Aplica la estrategia declarada en el contrato
4. Filtra columnas con `_select_for_silver()` — Bronze metadata no pasa a Silver
5. Añade `_silver_modified_at` si el contrato lo pide

---

### Módulo 2: `table_governance` (Silver → Gold)

```
table_governance/
├── contracts/
│   ├── loader.py          # JSON → TableContract (frozen dataclass)
│   └── validator.py       # SchemaValidator — tipos y nulabilidad
├── writers/
│   ├── table_writer.py    # ★ Fachada pública
│   ├── base_writer.py     # Bridge local ↔ Databricks + merge_schema + masks
│   ├── create_writer.py   # CREATE OR REPLACE TABLE + SET MASK
│   ├── append_writer.py   # INSERT INTO (soporta mergeSchema)
│   ├── upsert_writer.py   # MERGE INTO (SCD1)
│   ├── partition_writer.py# overwrite_partition (soporta mergeSchema)
│   └── delete_writer.py   # DELETE WHERE
└── migrations/
    └── safe_migrator.py   # Compara contrato vs estado real → ALTER TABLE
```

---

## Flujo de una escritura

```mermaid
sequenceDiagram
    participant P as Pipeline
    participant TW as TableWriter
    participant SV as SchemaValidator
    participant BW as BaseWriter
    participant D as Delta Lake

    P->>TW: overwrite(df)
    TW->>SV: validate(df, contract)
    SV-->>TW: OK / ValidationError
    TW->>BW: _write_df(df, "overwrite")
    BW->>D: DataFrameWriter.format("delta").mode("overwrite")
    D-->>BW: commit
    BW->>D: ALTER COLUMN COMMENT
    BW->>D: SET TBLPROPERTIES
    BW->>D: SET MASK (solo Databricks)
    D-->>P: done
```

---

## Flujo de promoción Silver (estrategia cdc_merge)

```mermaid
sequenceDiagram
    participant SP as SilverPromoter
    participant CM as CdcMergeStrategy
    participant B as Bronze (Delta)
    participant S as Silver (Delta)

    SP->>CM: execute()
    CM->>B: read() — lee todos los eventos
    CM->>CM: filter op_type IN (I, U) → upserts
    CM->>CM: filter op_type = D → deletes
    CM->>CM: withColumn("is_deleted", False)
    CM->>CM: withColumn("_silver_modified_at", now())
    CM->>CM: _select_for_silver() — excluye _ingested_at, _source_file
    CM->>S: MERGE INTO (upserts)
    CM->>CM: withColumn("is_deleted", True) — soft delete
    CM->>S: MERGE INTO (soft deletes)
    S-->>SP: filas en Silver
```

---

## Runtime-agnóstico: local ↔ Databricks

```mermaid
flowchart LR
    CF["config.json\nEXECUTION_ENVIRONMENT"] --> L["Launcher"]
    L --> E{{"is_databricks?"}}
    E -->|"false"| LC["SparkSession local\ncatalog = schema.name\npaths = /tmp/..."]
    E -->|"true"| DC["Databricks Connect\ncatalog = catalog.schema.name\npaths = abfss://..."]
    LC & DC --> TW["TableWriter / IngestionEngine\n(mismo código)"]
```

El `Launcher` se instancia **una vez** como singleton del proceso. Todos los writers, readers, ingestors y el `SafeMigrator` obtienen `spark` y `env` internamente vía `Launcher.current()`.

---

## Descripción de componentes

### Core

| Módulo | Responsabilidad |
|---|---|
| `Launcher` | Punto de entrada único. Detecta runtime, crea `SparkSession`, se registra como singleton. |
| `EnvironmentConfig` | Resuelve placeholders `{catalog.bronze}`, `{path.silver}` según el ambiente activo. |
| `LoggerConfig` | Logging estructurado con `loguru`. Mixin `LoggableMixin` inyecta `self.log` en cualquier clase. |

### Ingestion

| Módulo | Responsabilidad |
|---|---|
| `IngestionEngine` | Orquestador. Factory `from_spark()`. Métodos `ingest_bronze()`, `run_streaming()`, `promote_silver()`, `status()`. |
| `BronzeIngestor` | Lee archivos Landing, añade metadata, escribe Bronze con partition overwrite. |
| `SilverPromoter` | Aplica la estrategia declarada en el contrato Silver. |
| `FileReader` | Lectura batch de archivos (JSON, CSV, Parquet, Delta). |
| `FileStreamReader` | Lectura streaming — infiere schema desde archivos existentes si no se declara uno. |
| `FullMergeStrategy` | MERGE INTO Silver con dedup por watermark. SCD Type 1. |
| `CdcMergeStrategy` | Aplica I/U/D desde Bronze. Soft delete via `is_deleted`. |
| `IncrementalReplaceStrategy` | Filtra la partición más reciente del Bronze y hace upsert. |
| `AppendDedupStrategy` | Anti-join: solo inserta registros que no existen en Silver. |

### Table Governance

| Módulo | Responsabilidad |
|---|---|
| `TableContract` | Dataclass inmutable (frozen). Representa el estado deseado de una tabla. |
| `SchemaValidator` | Compara tipos Spark del DataFrame contra el contrato. Soporta widening. |
| `TableWriter` | Fachada pública: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`. |
| `TableReader` | Lectura gobernada: `read()`, `read_partition()`, `read_stream()`, `read_cdf()`. |
| `SafeMigrator` | Compara contrato vs. tabla real. Genera plan `ALTER TABLE` sin pérdida de datos. |
