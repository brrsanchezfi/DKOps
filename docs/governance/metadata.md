# Metadata y tablas externas

## apply_contract_metadata

El contrato documenta la tabla y sus columnas, pero esa información solo llega al
catálogo cuando alguien la aplica. `overwrite()` lo hace automáticamente. Para el resto
de casos existe `apply_contract_metadata()`:

```python
TableWriter(contract).apply_contract_metadata()
```

Es idempotente y no reescribe datos. Aplica lo siguiente:

| Elemento del contrato | Sentencia | Dónde tiene efecto |
|---|---|---|
| `comment` de la tabla | `COMMENT ON TABLE` | Local y Databricks |
| `comment` de cada columna | `ALTER TABLE ... ALTER COLUMN ... COMMENT` | Local y Databricks |
| `mask` de columna | `ALTER TABLE ... SET MASK` | Solo Databricks |
| `permissions` | `GRANT` o `REVOKE` | Solo Databricks |

Si la tabla no existe, registra un aviso y no hace nada; nunca lanza una excepción. Con
`dry_run=True` no ejecuta ninguna sentencia.

### Cuándo llamarla

No hace falta llamarla después de `overwrite()`, de la primera carga de `upsert()` ni de
la escritura streaming de `BronzeIngestor`: esos caminos ya lo hacen.

Sí es útil para **reparar tablas existentes** después de cambiar comentarios en el
contrato, sin tener que recrearlas:

```python
from pathlib import Path
from DKOps.table_governance import load_contract, TableWriter

for path in Path("tables/silver").glob("*.json"):
    TableWriter(load_contract(path)).apply_contract_metadata()
```

## Tablas EXTERNAL

Si el contrato declara `"type": "EXTERNAL"` y una `location`, la tabla se crea en esa
ruta sea cual sea la operación que la cree, no solo con `overwrite()`.

```json
{
  "type":     "EXTERNAL",
  "location": "{path.bronze}/batch/ventas_raw",
  "columns":  [ ]
}
```

!!! warning "La ubicación solo se aplica al crear la tabla"

    Si la tabla ya existe, DKOps no le pasa la ruta. Spark rechazaría la escritura si la
    ubicación no coincide y eso rompería un pipeline que funcionaba.

    Convertir tablas MANAGED creadas antes de la v0.3.3 en EXTERNAL es una migración que
    hay que hacer a mano. Cambiar el contrato no mueve los datos.
