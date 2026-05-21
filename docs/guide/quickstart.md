# Quickstart

## 1. config.json

El `config.json` define la configuración de infraestructura y la política de logs.
El nombre del archivo de log **no** va aquí — se pasa directamente al `Launcher`.

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME": "DKOps",
  "SPARK_WAREHOUSE_DIR": "/tmp/spark-warehouse",
  "DELTA_VERSION": "3.2.0",

  "LOG_LEVEL":     "INFO",
  "LOG_DIR":       "/tmp/logs",
  "LOG_ROTATION":  "10 MB",
  "LOG_RETENTION": "7 days",
  "LOG_SERIALIZE": false,

  "environments": {
    "dev": {
      "catalogs": {"bronze": "ct_bronze_dev"},
      "paths": {"raw": "abfss://raw@storage.dfs.core.windows.net"}
    }
  }
}
```

`LOG_DIR` acepta rutas locales, DBFS (`/dbfs/...`, `dbfs:/...`) y rutas de
storage account (`abfss://contenedor@cuenta.dfs.core.windows.net/logs`).

## 2. Contrato de tabla

```json
{
  "catalog": "{catalog.bronze}",
  "schema": "mi_schema",
  "name": "mi_tabla",
  "type": "MANAGED",
  "format": "DELTA",
  "columns": [
    {"name": "id",    "type": "STRING", "nullable": false},
    {"name": "fecha", "type": "DATE"},
    {"name": "cargado_en", "type": "TIMESTAMP", "default": "current_timestamp()"}
  ],
  "partitions": ["fecha"]
}
```

## 3. Pipeline

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter, AppendWriter, UpsertWriter

# log_filename identifica este ETL en los archivos de log.
# Si se omite, se usa SPARK_APP_NAME del config.json.
launcher = Launcher("config/config.json", log_filename="miEtl")
contract = load_contract("tables/mi_tabla.json", launcher.env)

CreateWriter(launcher.spark, contract, launcher.env).write(df)
AppendWriter(launcher.spark, contract, launcher.env).write(df_nuevo)
UpsertWriter(launcher.spark, contract, launcher.env).write(df_corr, merge_keys=["id"])
```
