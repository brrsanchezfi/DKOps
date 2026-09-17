<div align="center">

# DKOps

**Gobierno de tablas Delta y orquestación de pipelines Spark, en local y en Databricks.**

[![PyPI](https://img.shields.io/pypi/v/DKOps.svg)](https://pypi.org/project/DKOps/)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.2-00ADD4.svg)](https://delta.io/)
[![Docs](https://img.shields.io/badge/docs-online-0f766e.svg)](https://brrsanchezfi.github.io/DKOps/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

[Documentación](https://brrsanchezfi.github.io/DKOps/) ·
[Primeros pasos](https://brrsanchezfi.github.io/DKOps/getting-started/) ·
[Demos](https://brrsanchezfi.github.io/DKOps/demos/) ·
[Changelog](CHANGELOG.md)

</div>

---

DKOps es un framework en Python para construir lakehouses Delta con la arquitectura
Medallion (Landing, Bronze, Silver y Gold). Describes tus tablas y tus cargas en archivos
JSON versionados, y el framework se encarga de crearlas, validarlas, llenarlas y
mantenerlas. El mismo código corre en tu equipo y en Databricks.

## Por qué

Cuando un equipo de datos pasa de unos cuantos scripts a decenas de tablas, aparecen
siempre los mismos problemas. DKOps los aborda así:

| Problema | Cómo lo resuelve |
|---|---|
| El schema de cada tabla está escondido en el código | Contratos JSON validados antes de cada escritura |
| Un cambio de columnas rompe la carga | `SafeMigrator` y la opción `merge_schema` |
| Cada dataset repite la misma lógica de ingesta | `IngestionEngine` con estrategias que se eligen en el contrato |
| El código de local y el de Databricks divergen | Detección de runtime y placeholders por entorno |
| Nadie sabe qué corrió anoche ni cuántas filas movió | Tabla de control operativo consultable con SQL |

## Instalación

```bash
# Desarrollo local, incluye PySpark y Delta
pip install "DKOps[local]"

# Desde tu equipo contra un cluster, con Databricks Connect
pip install "DKOps[databricks-connect]"

# Dentro de Databricks, donde Spark y Delta ya están instalados
pip install DKOps
```

El módulo se importa como `DKOps`, respetando mayúsculas: `import dkops` no funciona.

Si instalas desde un tag de git, usa `v0.3.2` o posterior. Los tags `v0.3.0` y `v0.3.1`
tienen problemas de empaquetado que no afectan a los paquetes de PyPI; los detalles están
en la [guía de instalación](https://brrsanchezfi.github.io/DKOps/getting-started/installation/).

## Un vistazo rápido

### Ingesta de Landing a Silver

```python
from DKOps.launcher import Launcher
from DKOps.ingestion.engine import IngestionEngine

Launcher("config/config.json")

engine = IngestionEngine.from_launcher(
    bronze_contracts_dir    = "ingestion/batch",
    streaming_contracts_dir = "ingestion/streaming",
    silver_contracts_dir    = "ingestion/silver",
    tables_base_dir         = ".",
    ops_path                = "/tmp/ops/control",
)

engine.ingest_bronze()     # de Landing a Bronze
engine.run_streaming()     # de Landing a Bronze en streaming
engine.promote_silver()    # de Bronze a Silver
engine.status()
```

### Escritura y lectura gobernadas

```python
from DKOps.table_governance import load_contract, TableWriter, TableReader, SafeMigrator

contract = load_contract("tables/silver/ventas_current.json")

writer = TableWriter(contract)
writer.overwrite(df)
writer.upsert(df_cambios, keys=["venta_id"])
writer.overwrite_partition(df_dia, {"fecha": "2024-01-15"})

reader = TableReader(contract)
df = reader.read(filter="estado = 'activo'")
cambios = reader.read_cdf(starting_version=1)

SafeMigrator(contract, dry_run=True).apply()   # muestra el plan sin ejecutar
```

## Contratos

Toda la configuración vive en dos tipos de contrato JSON.

**Contrato de tabla**, en `tables/{bronze,silver,gold}/`. Describe cómo es la tabla:

```json
{
  "catalog": "{catalog.silver}",
  "schema":  "ventas",
  "name":    "ventas_current",
  "columns": [
    { "name": "venta_id",   "type": "STRING",  "nullable": false },
    { "name": "email",      "type": "STRING",  "mask": "security.mask_email" },
    { "name": "is_deleted", "type": "BOOLEAN" }
  ],
  "partitions": ["canal"],
  "properties": { "change_data_feed": true }
}
```

**Contrato de ingesta**, en `ingestion/{batch,streaming,silver}/`. Describe cómo se llena:

```json
{
  "name":                 "ventas_current",
  "strategy":             "cdc_merge",
  "source_contract":      "../../tables/bronze/ventas_raw.json",
  "destination_contract": "../../tables/silver/ventas_current.json",
  "merge_keys":           ["venta_id"],
  "watermark_col":        "fecha_venta"
}
```

Los placeholders `{catalog.<capa>}`, `{path.<nombre>}`, `{env}` y `{env_short}` se
resuelven con el bloque `environments` de `config.json`, así el mismo contrato sirve para
desarrollo y producción. Los JSON Schema de ambos tipos están en `schema/` y se validan
con `python scripts/validate_contracts.py`.

## Estrategias de promoción a Silver

| Estrategia | Cuándo usarla |
|---|---|
| `full_merge` | Catálogos y dimensiones que llegan completos en cada entrega |
| `cdc_merge` | Eventos de cambio con `op_type` I/U/D; aplica borrado lógico con `is_deleted` |
| `incremental_replace` | Snapshots diarios en los que solo cuenta la partición más reciente |
| `append_dedup` | Eventos, logs o clickstream que nunca se actualizan |

## Idempotencia

Los pipelines se pueden ejecutar varias veces sin duplicar datos. Bronze sobrescribe la
partición `_ingested_date` del día, Silver hace upsert por clave de negocio y el streaming
guarda checkpoints.

## Demos

El repositorio trae cinco proyectos completos, cada uno con su generador de datos:

| Demo | Dominio | Lo más destacado |
|---|---|---|
| `demo_1` | Aeronáutica | Los cinco writers y `SafeMigrator` |
| `demo_2` | Manufactura | Reglas de calidad declarativas y transformaciones con tests |
| `demo_3` | E-commerce | `merge_schema`, máscaras de columna y streaming |
| `demo_4` | Retail e inventario | `read_cdf()`, `read_stream()` y `SafeMigrator` |
| `demo_5` | Marketplace | Flujo completo hasta Gold con tabla de control |

```bash
python demos/demo_5/pipeline.py
```

Los demos escriben en `/tmp/dkops_demoN/`. Borra esa carpeta si quieres empezar de cero.

## Estructura del repositorio

```
src/DKOps/
├── launcher.py              SparkSession y detección de runtime
├── environment_config.py    catálogos, rutas y placeholders
├── logger_config.py         logging con Loguru
├── ingestion/               motor de ingesta, readers y estrategias
└── table_governance/        contratos, writers, reader y migraciones
schema/                      JSON Schema de los contratos
demos/                       cinco proyectos de ejemplo
docs/                        sitio de documentación (MkDocs)
tests/                       suite de pytest
```

## Desarrollo

```bash
git clone https://github.com/brrsanchezfi/DKOps
cd DKOps
pip install -e ".[local]"

python -m pytest -q                       # tests unitarios
python -m pytest tests/integration -q     # integración con Spark y Delta reales
```

Los tests de integración deben correr en un proceso aparte: los tests unitarios
sustituyen `pyspark` por un mock en `sys.modules`.

Para trabajar en la documentación:

```bash
pip install mkdocs-material "mkdocstrings[python]"
mkdocs serve
```

`pyspark` y `databricks-connect` no pueden convivir en el mismo entorno virtual. Si usas
ambos, mantén dos entornos separados.

## Licencia

MIT. Consulta el archivo [LICENSE](LICENSE).
