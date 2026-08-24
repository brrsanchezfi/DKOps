# DKOps — guía para Claude

Framework Python de gobernanza y automatización de lakehouses Delta (Medallion:
Landing → Bronze → Silver → Gold). El mismo código corre en PC local y en
Databricks; el framework detecta el runtime.

## Layout

```
src/DKOps/
  launcher.py            Launcher — crea la SparkSession y el EnvironmentConfig.
                         Launcher.current() es el singleton que usan los loaders.
  environment_config.py  Resuelve catalogs/paths/env desde config.json.
  logger_config.py       AppLogger + LoggableMixin (self.log). Basado en Loguru.

  ingestion/             Módulo de ingesta (Landing → Bronze → Silver)
    engine.py              IngestionEngine — orquestador y punto de entrada único.
    bronze_ingestor.py     Landing → Bronze, partition overwrite por _ingested_date.
    silver_promoter.py     Bronze → Silver, delega en una estrategia.
    contracts/             IngestionContract (dataclass) + IngestionContractLoader.
    readers/               BaseSourceReader + SourceReaderFactory + implementaciones.
    strategies/            BasePromotionStrategy + 4 estrategias de promoción.
    enrichment/metadata.py MetadataEnricher — columnas técnicas.
    ops/ops_logger.py      Tabla de control operativo (run_id, filas, estado).

  table_governance/      Módulo de gobierno de tablas
    contracts/loader.py    TableContract + ContractLoader (+ load_contract()).
    contracts/validator.py SchemaValidator — contrato vs. DataFrame.
    writers/               TableWriter (fachada) + writers por operación.
    readers/table_reader.py TableReader — read / read_partition / read_stream / read_cdf.
    migrations/            SafeMigrator — plan de cambios sin pérdida de datos.

schema/                  JSON Schema de los contratos (ver más abajo).
demos/demo_1..demo_5/    Demos end-to-end: datagen/ + pipeline.py + contratos.
tests/                   pytest. tests/ingestion/ para el módulo de ingesta.
```

## Contratos JSON — el corazón del framework

Hay **dos tipos** de contrato y no deben confundirse:

| Tipo | Vive en | Describe | Cargado por |
|---|---|---|---|
| **Table contract** | `demos/*/tables/{bronze,silver,gold}/*.json` | schema, particiones, permisos, propiedades Delta de una tabla | `ContractLoader` |
| **Ingestion contract** | `demos/*/ingestion/{batch,streaming,silver}/*.json` | de dónde vienen los datos, a qué tabla van y con qué estrategia | `IngestionContractLoader` |

Un ingestion contract **referencia** table contracts por ruta relativa
(`"destination_contract": "../../tables/bronze/ventas_raw.json"`), resuelta
respecto al directorio del propio JSON de ingesta — no respecto a `base_dir`.

**Placeholders** resueltos por ambos loaders: `{catalog.<capa>}`, `{path.<nombre>}`,
`{env}`, `{env_short}`. Los valores vienen de `environments.<target>` en el
`config.json` del demo. Un placeholder no definido lanza `KeyError`.

Los JSON Schema en `schema/` son la fuente de verdad para escribir contratos
nuevos. Valídalos con:

```bash
python scripts/validate_contracts.py
```

## Reglas del proyecto

- **PySpark y Delta NO van en `dependencies`** de `pyproject.toml`. En Databricks
  ya están instalados nativamente y añadirlos rompe el cluster. Van en el extra
  `[local]`.
- **Nunca instanciar dataclasses de contrato a mano** (`TableContract`,
  `IngestionContract`). Son `frozen` y las construyen los loaders.
- **Logging vía `LoggableMixin`**: hereda de él y usa `self.log.info(...)`. No usar
  `print()` ni `logging` directamente en `src/`.
- **Idempotencia**: los pipelines deben poder correr N veces sin duplicar datos.
  Bronze usa partition overwrite por `_ingested_date`; Silver usa upsert;
  streaming usa checkpoints.
- Los contratos aceptan una clave `_doc` como comentario. Ignórala al parsear.
- Docstrings y comentarios del código están en español — mantén ese idioma.

## Comandos

```bash
python -m pytest -q
```

Los tests de `tests/integration/` usan Spark y Delta reales y están excluidos
de la suite por defecto. Deben correr en su **propio proceso**: los módulos con
mocks registran un `MagicMock` bajo `pyspark` en `sys.modules`, así que
compartir intérprete dejaría un Spark falso.

```bash
python -m pytest tests/integration -q
```

```bash
python demos/demo_5/pipeline.py
```

Los demos escriben en `/tmp/dkops_demoN/`. Para empezar de cero borra ese
directorio antes de correr.

Entornos virtuales: `.venv-local` (PySpark local) y `.venv-databricks`
(Databricks Connect). Son excluyentes — `databricks-connect` y `pyspark` no
pueden convivir en el mismo venv.

## Skills disponibles

- `/nuevo-contrato-ingesta` — crea un ingestion contract (+ table contract destino).
- `/nueva-estrategia-silver` — añade una estrategia de promoción Bronze → Silver.
- `/nuevo-reader` — añade un reader de fuente nuevo.
- `/validar-contratos` — valida todos los JSON contra los schemas.
