# Changelog

All notable changes to DKOps are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) · Versioning: [Semantic Versioning](https://semver.org/).

---

## [0.3.1] — 2026-08-23

### Added

- **`TableWriter.apply_contract_metadata()`** — aplica comentario de tabla, comentarios de columna, masks y permisos del contrato de forma idempotente y sin reescribir datos. Sirve tanto para los caminos de escritura que no pasan por `CreateWriter` como para reparar tablas ya creadas (#18)
- **`insert_only_columns`** en `TableWriter.upsert()` y `UpsertWriter.write()` — columnas que el MERGE inserta pero nunca actualiza (#19)
- **`_silver_created_at`** en la promoción Bronze → Silver — las cuatro estrategias añaden ahora las dos columnas que `add_silver_timestamps` promete (#19)
- Verificación en el workflow de publicación: el tag debe coincidir con `version` de `pyproject.toml` o el build falla (#20)

### Fixed

- La carga inicial de `UpsertWriter` (tabla inexistente) dejaba la tabla sin comentarios en el catálogo (#18)
- `BronzeIngestor._write_stream()` creaba la tabla via `writeStream.toTable()` sin aplicar la metadata del `TableContract` (#18)
- `add_silver_timestamps` producía dos columnas en `MetadataEnricher` pero solo `_silver_modified_at` en las estrategias de promoción, de modo que un contrato Silver que declarara ambas fallaba la validación (#19)
- El MERGE de `UpsertWriter` actualizaba todas las columnas no-key, lo que habría sobrescrito `_silver_created_at` en cada ejecución (#19)
- **`IngestionEngine.promote_silver()` omitía silenciosamente todas las promociones.** El engine resolvía el `TableContract` Bronze de cada contrato Silver desde su `source_contract` y luego descartaba el resultado; la búsqueda efectiva se hacía por nombre contra `_bronze_tables` y no encontraba nada salvo que el contrato de ingesta batch, la tabla Bronze y el contrato Silver se llamaran igual. Los cinco demos salían con `Faltan contratos src/dst — omitido` y Silver quedaba vacío. El mensaje de WARNING ahora dice cuál de los dos contratos no se pudo resolver

### Tests

- Nueva suite `tests/integration/` con Spark y Delta reales — verifica que `_silver_created_at` sobrevive a un segundo MERGE y que `apply_contract_metadata()` deja los comentarios visibles en `DESCRIBE TABLE`. Excluida de la suite por defecto: debe correr en su propio proceso (`pytest tests/integration`)
- 18 tests nuevos de mocks sobre `apply_contract_metadata`, `insert_only_columns` y las columnas técnicas de Silver

### Notes

- El tag `v0.3.0` empaqueta un `pyproject.toml` que declara `0.2.4`: se creó antes del commit de bump. Se corrige publicando `0.3.1`; el tag `v0.3.0` se deja intacto para no romper instalaciones existentes (#20)

---

## [0.3.0] — 2026-05-23

### Added

- **`IngestionEngine`** — orquestador principal: `ingest_bronze()`, `run_streaming()`, `promote_silver()`, `status()`
- **`BronzeIngestor`** — ingesta Landing → Bronze con partition overwrite idempotente por `_ingested_date`
- **`SilverPromoter`** — aplica estrategias declarativas desde contratos JSON
- **Estrategia `full_merge`** — MERGE INTO con dedup por watermark (SCD Type 1)
- **Estrategia `cdc_merge`** — CDC I/U/D con soft delete via `is_deleted`
- **Estrategia `incremental_replace`** — upsert de la partición más reciente por watermark
- **Estrategia `append_dedup`** — anti-join append para eventos y clickstream
- **`FileStreamReader`** — lectura streaming con auto-inferencia de schema desde archivos existentes
- **`LoadType.STREAMING`** — tipo de carga semántico para contratos streaming
- **Tabla de control operativo** — registro por dataset de filas, estado, timestamps y run_id
- **5 demos end-to-end verificados** — Aeronáutica, Manufactura, E-commerce, Retail, Marketplace
- **Documentación completa** — diagramas Mermaid, guía de ingesta, quickstart actualizado, 5 páginas de demos

### Fixed

- `CdcMergeStrategy._apply_deletes()` — añade `_silver_modified_at` y aplica `_select_for_silver()` en soft deletes
- `CdcMergeStrategy` — añade `is_deleted=False` en upserts cuando la columna está en el contrato Silver
- `AppendDedupStrategy` — añade `_silver_modified_at` antes de `_select_for_silver()`
- `IncrementalReplaceStrategy` — añade `_silver_modified_at` antes de `_select_for_silver()`
- `FileStreamReader` — `readStream` ahora infiere schema desde archivos estáticos existentes (evita `AnalysisException`)
- Contratos demo_2/demo_5 — tipos de columna alineados con lo que Spark `inferSchema` produce (STRING vs DATE/TIMESTAMP)

### Changed

- Versión de desarrollo Alpha → **Beta** (`Development Status :: 4 - Beta`)
- Descripción del paquete actualizada para reflejar IngestionEngine y arquitectura Medallion
- URLs del proyecto apuntan a GitHub Pages en lugar del repositorio raw

### Removed

- Scripts obsoletos `pipeline_aeronautica.py`, `pipeline_manufactura.py`, `pipeline_ecommerce.py`, `pipeline_lectura.py`
- `data_generator.py` ×4 (reemplazados por directorios `datagen/` por demo)
- Directorio `build/` (artefactos compilados)

---

## [0.2.4] — anterior

- `TableWriter` — API unificada: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`
- `TableReader` — `read()`, `read_partition()`, `read_stream()`, `read_cdf()`
- `SafeMigrator` — comparación contrato vs estado real con plan `ALTER TABLE`
- `ContractLoader` — carga y resolución de placeholders en contratos JSON
- `SchemaValidator` — validación de tipos y nulabilidad pre-escritura
- Runtime detector local / Databricks — mismo código sin cambios
