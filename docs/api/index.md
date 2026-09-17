# Referencia API

<p class="dk-lead">
Documentación generada a partir de los docstrings del código fuente. Si buscas cómo usar
una clase, las guías de cada sección son un mejor punto de partida; esta referencia es
útil para consultar parámetros y firmas exactas.
</p>

## Núcleo

| Clase | Módulo | Descripción |
|---|---|---|
| [`Launcher`](launcher.md) | `DKOps.launcher` | Crea la SparkSession y el EnvironmentConfig |
| [`EnvironmentConfig`](environment_config.md) | `DKOps.environment_config` | Resuelve catálogos, rutas y entorno |
| [`AppLogger`, `LoggableMixin`](logger_config.md) | `DKOps.logger_config` | Logging estructurado |

## Ingesta

| Clase | Módulo | Descripción |
|---|---|---|
| [`IngestionEngine`](ingestion/engine.md) | `DKOps.ingestion.engine` | Orquestador de la ingesta |
| [`BronzeIngestor`](ingestion/bronze_ingestor.md) | `DKOps.ingestion.bronze_ingestor` | De Landing a Bronze |
| [`SilverPromoter`](ingestion/silver_promoter.md) | `DKOps.ingestion.silver_promoter` | De Bronze a Silver |
| [Estrategias](ingestion/strategies.md) | `DKOps.ingestion.strategies` | Las cuatro estrategias de promoción |

## Gobierno de tablas

| Clase | Módulo | Descripción |
|---|---|---|
| [`ContractLoader`, `TableContract`](contracts/loader.md) | `DKOps.table_governance.contracts.loader` | Carga de contratos de tabla |
| [`SchemaValidator`](contracts/validator.md) | `DKOps.table_governance.contracts.validator` | Validación de DataFrames |
| [`TableWriter`](writers/table_writer.md) | `DKOps.table_governance.writers.table_writer` | Fachada de escritura |
| [`TableReader`](readers/table_reader.md) | `DKOps.table_governance.readers.table_reader` | Fachada de lectura |
| [`SafeMigrator`](migrations/safe_migrator.md) | `DKOps.table_governance.migrations.safe_migrator` | Migraciones seguras |

Todo lo que se usa habitualmente se puede importar desde `DKOps.table_governance`:

```python
from DKOps.table_governance import (
    load_contract, TableWriter, TableReader, SafeMigrator,
)
```
