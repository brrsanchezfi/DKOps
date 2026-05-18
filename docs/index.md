**Lakehouse automation framework para DataOps, DevOps y arquitectura Medallion en Databricks.**

## Características

- **Contratos de tabla** — define schema, tipos, particiones, permisos y reglas de gobernanza en JSON versionado
- **Validación automática** — verifica tipos y nulabilidad antes de escribir
- **`TableWriter`** — API unificada: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`
- **`TableReader`** — lectura gobernada: `read`, `read_partition`, `read_stream`, `read_cdf`; devuelve `DataFrame` nativo de PySpark
- **Change Data Feed** — declara `"delta.enableChangeDataFeed": "true"` en el contrato y captura inserts, updates y deletes por versión
- **merge_schema** — evolución de schema automática: declara `"merge_schema": true` en el contrato y Delta añade columnas nuevas sin recrear la tabla
- **Enmascaramiento de columnas** — declara `"mask": "security.fn"` en una columna y el framework aplica `SET MASK` post-escritura en Unity Catalog
- **Dual runtime** — mismo código en PC local y en Databricks sin cambios
- **Migraciones seguras** — `SafeMigrator` compara contrato vs estado real y genera plan sin pérdida de datos
- **Logging estructurado** — cada operación loguea inicio, fin y duración

## Quickstart

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter, TableReader

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_ventas.json")

# ── Escritura ──────────────────────────────────────────────────────────────
TableWriter(contract).overwrite(df)                               # full load
TableWriter(contract).upsert(df_delta, keys=["venta_id"])         # MERGE INTO
TableWriter(contract).overwrite_partition(df_day,                 # partición
    partition={"fecha": "2024-01-15"})

# ── Lectura ────────────────────────────────────────────────────────────────
reader = TableReader(contract)

df      = reader.read(filter="fecha >= '2024-01-01'")             # con filtro
df_part = reader.read_partition({"fecha": "2024-01-15"})          # solo partición
stream  = reader.read_stream()                                    # Structured Streaming
cdf     = reader.read_cdf(starting_version=1)                     # Change Data Feed
```

## Arquitectura
