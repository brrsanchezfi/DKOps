# Quickstart

## 1. config.json

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME": "DKOps",
  "SPARK_WAREHOUSE_DIR": "/tmp/spark-warehouse",
  "DELTA_VERSION": "3.2.0",
  "LOG_LEVEL": "INFO",
  "environments": {
    "dev": {
      "catalogs": {"bronze": "ct_bronze_dev"},
      "paths": {"raw": "abfss://raw@storage.dfs.core.windows.net"}
    }
  }
}
```

## 2. Contrato de tabla

```json
{
  "catalog": "{catalog.bronze}",
  "schema":  "mi_schema",
  "name":    "fact_ventas",
  "type":    "MANAGED",
  "format":  "DELTA",
  "comment": "Ventas diarias",
  "columns": [
    {"name": "venta_id",    "type": "STRING",    "nullable": false},
    {"name": "fecha",       "type": "DATE",       "nullable": false},
    {"name": "producto_id", "type": "STRING",     "nullable": false},
    {"name": "monto_usd",   "type": "DOUBLE"},
    {"name": "cargado_en",  "type": "TIMESTAMP",  "default": "current_timestamp()"}
  ],
  "partitions": ["fecha"],
  "properties": {
    "delta.autoOptimize.optimizeWrite": "true"
  }
}
```

## 3. Escritura

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_ventas.json")

# Carga full (CREATE OR REPLACE)
TableWriter(contract).overwrite(df)

# Incremental — upsert por clave
TableWriter(contract).upsert(df_delta, keys=["venta_id", "fecha"])

# Partición específica
TableWriter(contract).overwrite_partition(df_day, partition={"fecha": "2024-01-15"})

# Append — schema evolution si merge_schema=true
TableWriter(contract).append(df_nuevo)
```

## 4. Lectura

```python
from DKOps.table_governance import load_contract, TableReader

contract = load_contract("tables/fact_ventas.json")
reader   = TableReader(contract)

# Tabla completa
df = reader.read()

# Con filtro y proyección
df = reader.read(
    filter="fecha >= '2024-01-01'",
    columns=["venta_id", "fecha", "monto_usd"],
)

# Partición eficiente
df = reader.read_partition({"fecha": "2024-01-15"})

# Streaming (Delta log)
stream = reader.read_stream()
query  = stream.writeStream.foreachBatch(mi_fn).trigger(availableNow=True).start()
query.awaitTermination()
```

## 5. Change Data Feed

Requiere `"delta.enableChangeDataFeed": "true"` en el contrato:

```json
{
  "properties": {
    "delta.enableChangeDataFeed": "true"
  }
}
```

```python
reader = TableReader(load_contract("tables/inventario.json"))

# Cambios desde versión 1
df_cambios = reader.read_cdf(starting_version=1)
df_cambios.select("producto_id", "_change_type", "_commit_version").show()
```

## 6. Migraciones seguras

```python
from DKOps.table_governance import load_contract
from DKOps.table_governance.migrations import SafeMigrator

contract = load_contract("tables/fact_ventas.json")
migrator = SafeMigrator(contract)

# Ver qué cambiaría
plan = migrator.plan()
plan.print()

# Aplicar (solo agrega columnas, nunca las borra)
migrator.apply()
```

## Imports disponibles

```python
# API pública recomendada
from DKOps.launcher import Launcher
from DKOps.table_governance import (
    load_contract,
    TableWriter,   # escritura
    TableReader,   # lectura
)
from DKOps.table_governance.migrations import SafeMigrator
```
