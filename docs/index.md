**Lakehouse automation framework para DataOps, DevOps y arquitectura Medallion en Databricks.**

## Características

- **Contratos de tabla** — define schema, tipos, particiones, permisos y reglas de gobernanza en JSON versionado
- **Validación automática** — verifica tipos y nulabilidad antes de escribir
- **`TableWriter`** — API unificada: `overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`
- **merge_schema** — evolución de schema automática: declara `"merge_schema": true` en el contrato y Delta añade columnas nuevas sin recrear la tabla
- **Enmascaramiento de columnas** — declara `"mask": "security.fn"` en una columna y el framework aplica `SET MASK` post-escritura en Unity Catalog
- **Dual runtime** — mismo código en PC local y en Databricks
- **Migraciones seguras** — `SafeMigrator` compara contrato vs estado real y genera plan sin pérdida de datos
- **Logging estructurado** — cada operación loguea inicio, fin y duración

## Quickstart

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_ventas.json")

# Carga full
TableWriter(contract).overwrite(df)

# Incremental — upsert por clave
TableWriter(contract).upsert(df_delta, keys=["venta_id", "fecha"])

# Schema evolution — nuevas columnas sin recrear la tabla
# (requiere "merge_schema": true en el contrato)
TableWriter(contract).append(df_con_cols_nuevas)
```

## Arquitectura
