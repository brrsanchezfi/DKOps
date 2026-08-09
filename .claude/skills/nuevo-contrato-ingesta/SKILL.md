---
name: nuevo-contrato-ingesta
description: Crea un contrato de ingesta DKOps (batch, streaming o promoción Silver) junto con su table contract destino. Usar cuando se pida "ingestar un dataset nuevo", "añadir una tabla al pipeline", "crear un contrato de ingesta/de tabla", "promover algo a Silver", o al conectar una fuente nueva (Landing, Kafka, archivos) a Bronze.
---

# Nuevo contrato de ingesta

Genera los JSON que el `IngestionEngine` consume. Un dataset nuevo casi siempre
necesita **dos** archivos: el table contract destino y el ingestion contract.

## 1. Determinar el caso

| Caso | Señal | Archivos a crear |
|---|---|---|
| **Bronze batch** | archivos que llegan por lotes a Landing | `tables/bronze/<x>_raw.json` + `ingestion/batch/<x>.json` |
| **Bronze streaming** | flujo continuo, Kafka o Auto Loader | `tables/bronze/<x>_raw.json` + `ingestion/streaming/<x>.json` |
| **Promoción Silver** | ya existe en Bronze, se quiere limpio/deduplicado | `tables/silver/<x>_current.json` + `ingestion/silver/<x>_current.json` |

Si el usuario pide "ingestar X y dejarlo listo para consumo", son los tres pasos:
Bronze batch **y** promoción Silver.

Pregunta solo lo que no puedas deducir del repo. Lo mínimo que necesitas saber:
el nombre del dataset, sus columnas con tipos, y —para Silver— la clave de
negocio y qué columna ordena temporalmente los registros.

## 2. Leer los schemas antes de escribir

Los JSON Schema son la fuente de verdad de campos, enums y reglas condicionales:

- `schema/table_contract.schema.json`
- `schema/ingestion_contract.schema.json`

Léelos. Contienen los tipos válidos, los defaults reales y las descripciones de
cada campo. No inventes campos que no estén ahí — el loader los ignora en
silencio y el bug aparece en runtime.

Como referencia de estilo, el demo 5 tiene los tres casos resueltos:
`demos/demo_5/ingestion/{batch,streaming,silver}/` y `demos/demo_5/tables/`.

## 3. Elegir la estrategia Silver

| Estrategia | Cuándo | Requiere |
|---|---|---|
| `full_merge` | la fuente trae snapshots completos o filas actualizadas; se quiere el estado actual (SCD1) | `merge_keys`, `watermark_col` |
| `cdc_merge` | la fuente trae eventos I/U/D; hay que aplicar borrados (soft delete vía `is_deleted`) | `merge_keys`, `watermark_col` |
| `incremental_replace` | llega un snapshot por partición que reemplaza la anterior | partición en el contrato destino |
| `append_dedup` | solo inserciones, hay que evitar reprocesar claves ya vistas | `merge_keys` |

Ante la duda entre `full_merge` y `cdc_merge`: si la fuente puede indicar que un
registro fue **borrado** en origen, es `cdc_merge`.

## 4. Reglas que debes respetar

- **Rutas relativas al JSON de ingesta**, no al `base_dir`. Desde
  `ingestion/batch/` hacia `tables/bronze/` son dos niveles: `../../tables/bronze/x.json`.
- **Placeholders, nunca rutas literales**: `{path.landing}/<dataset>`,
  `{catalog.bronze}`. Deben existir en `environments.<target>` del `config.json`
  del demo — verifícalo, un placeholder no definido lanza `KeyError`.
- **Bronze siempre particiona por `_ingested_date`**. Es lo que hace idempotente
  la ingesta. Declara la columna en `columns` (tipo `DATE`) y en `partitions`, y
  deja `metadata.add_ingested_date` en true.
- **Metadatos técnicos por capa**: en Bronze activa `add_ingested_at`,
  `add_ingested_date`, `add_source_file`. En Silver usa
  `add_silver_timestamps: true` y **no** `add_source_file` — esa columna es de Bronze.
- **Silver declara solo columnas de negocio.** La estrategia hace `select` de las
  columnas del contrato Silver, así que los `_`-prefijados de Bronze se descartan
  solos. No los declares.
- **`change_data_feed: true`** en `properties` del contrato Bronze si algo va a
  leerlo con `TableReader.read_cdf()`.
- **Streaming local necesita `source.schema` explícito** — `FileStreamReader` no
  puede inferir el schema de un stream. En Databricks Auto Loader sí infiere.

## 5. Validar

Siempre, antes de dar por terminado:

```bash
python scripts/validate_contracts.py
```

Valida schema y chequeos cruzados (particiones declaradas, rutas que existen,
columnas duplicadas) sin arrancar Spark. Debe salir 0 errores.

## 6. Enganchar al pipeline

El `IngestionEngine` carga por directorio, así que un archivo nuevo en
`ingestion/batch/` se recoge solo con `ingest_bronze()`. Solo hay que tocar
`pipeline.py` si el usuario quiere ejecutar el dataset de forma aislada
(`ingest_bronze("<name>")`) o si hace falta una agregación Gold nueva.

Si el demo genera sus propios datos, revisa si hay que añadir un generador en
`datagen/` para que la fuente exista al correr el pipeline.
