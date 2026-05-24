# Changelog

All notable changes to DKOps are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) · Versioning: [Semantic Versioning](https://semver.org/).

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
