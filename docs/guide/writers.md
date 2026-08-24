# Writers

Todos los writers validan el schema antes de escribir y funcionan sin cambios en PC local y en Databricks.

## TableWriter — API principal

`TableWriter` es la fachada recomendada. Elige el writer correcto según el método que llames y pasa las opciones de configuración de forma uniforme.

```python
from DKOps.table_governance import load_contract, TableWriter

contract = load_contract("tables/fact_ventas.json")
writer   = TableWriter(contract)

writer.overwrite(df)                             # full load (CREATE OR REPLACE)
writer.append(df)                                # INSERT INTO
writer.upsert(df, keys=["venta_id", "fecha"])    # MERGE INTO (SCD1)
writer.overwrite_partition(df, partition={"fecha": "2024-01-15"})
writer.delete("fecha < '2023-01-01'")
writer.apply_contract_metadata()                 # comentarios, masks y permisos
```

### Opciones de construcción

| Parámetro | Tipo | Default | Descripción |
|---|---|---|---|
| `contract` | `TableContract` | — | Contrato cargado con `load_contract()` |
| `strict_columns` | `bool` | `True` | Falla si el DF tiene columnas extra no declaradas |
| `fail_on_warning` | `bool` | `False` | Trata advertencias del validador como errores |
| `dry_run` | `bool` | `False` | Simula la operación sin escribir nada |

```python
# Simular sin escribir
TableWriter(contract, dry_run=True).overwrite(df)

# Hacer la escritura estricta ante columnas extra
TableWriter(contract, fail_on_warning=True).append(df)
```

### upsert

```python
writer.upsert(
    df,
    keys=["id", "fecha"],          # columnas de join (obligatorio)
    update_columns=["estado"],     # si None, actualiza todas
)
```

#### Columnas que se insertan pero no se actualizan

Algunas columnas solo tienen sentido en el momento de la inserción: si el
`UPDATE` del MERGE las reescribiera en cada ejecución, dejarían de significar
nada. El caso típico es una marca de creación.

```python
writer.upsert(
    df,
    keys=["venta_id"],
    insert_only_columns=["_silver_created_at"],
)
```

La columna entra en el `WHEN NOT MATCHED THEN INSERT` y se excluye del
`WHEN MATCHED THEN UPDATE SET`. Si todas las columnas no-key quedaran excluidas,
el writer lanza `ValueError` en vez de emitir un MERGE que no actualiza nada.

Las estrategias de promoción a Silver ya lo usan automáticamente para
`_silver_created_at` — ver [Ingesta](ingestion.md#metadatos-silver).

### delete

```python
rows_deleted = writer.delete("fecha < '2023-01-01'")
rows_preview = writer.delete("estado = 'CANCELLED'", preview=True)  # no borra, solo cuenta
```

### apply_contract_metadata

El contrato documenta la tabla y cada columna, pero esa metadata solo llega al
catálogo cuando alguien la aplica. `overwrite()` lo hace por su cuenta; los
demás caminos no crean la tabla vía `CREATE TABLE`, así que necesitan una
llamada explícita:

```python
writer.apply_contract_metadata()
```

Aplica, de forma **idempotente** y sin reescribir los datos:

| Elemento | Sentencia emitida | Dónde aplica |
|---|---|---|
| `comment` de la tabla | `COMMENT ON TABLE` | Ambos runtimes |
| `comment` de cada columna | `ALTER TABLE ... ALTER COLUMN ... COMMENT` | Ambos runtimes |
| `mask` de columna | `ALTER TABLE ... SET MASK` | Solo Databricks (UC) |
| `permissions` | `GRANT` / `DENY` | Solo Databricks |

Dos usos:

- **Documentar tablas creadas por otro camino.** La primera carga de `upsert()`
  y la escritura streaming de `BronzeIngestor` ya la invocan internamente, así
  que no tienes que hacer nada.
- **Reparar tablas existentes** sin recrearlas — útil tras cambiar los
  `comment` del contrato:

  ```python
  for path in Path("tables/silver").glob("*.json"):
      TableWriter(load_contract(path)).apply_contract_metadata()
  ```

Si la tabla no existe, registra un WARNING y no hace nada — nunca lanza. En
`dry_run=True` no emite ninguna sentencia.

---

## merge_schema — Evolución de schema

Declara `"merge_schema": true` en el contrato para activar `mergeSchema=true` en operaciones append y partition overwrite. Permite añadir columnas nuevas sin recrear la tabla.

```json
{
  "catalog": "{catalog.silver}",
  "schema":  "ventas",
  "name":    "fact_ventas",
  "merge_schema": true,
  "columns": [...]
}
```

```python
TableWriter(contract).append(df_con_columnas_nuevas)  # no falla aunque el schema haya cambiado
```

!!! note
    `merge_schema` aplica en `append` y `overwrite_partition`. En `overwrite` siempre se usa `overwriteSchema=true` y no es necesario.

---

## Enmascaramiento de columnas

Declara `"mask"` en una columna para aplicar `ALTER TABLE … ALTER COLUMN … SET MASK` post-escritura. Solo se ejecuta en Databricks / Unity Catalog.

```json
{
  "name": "email",
  "type": "STRING",
  "mask": "security.mask_email"
}
```

La función de máscara debe existir previamente en Unity Catalog bajo el catálogo y schema indicados (`security.mask_email` se resuelve como `<catalog>.security.mask_email`).

El enmascaramiento se aplica automáticamente al crear la tabla (`overwrite`) y se omite silenciosamente en PC local o con `dry_run=True`.

---

## Writers individuales (API interna)

Los writers individuales siguen disponibles para casos avanzados, pero se recomienda usar `TableWriter`.

```python
from DKOps.table_governance.writers import (
    CreateWriter, AppendWriter, UpsertWriter, PartitionWriter, DeleteWriter
)

CreateWriter(contract).write(df)
AppendWriter(contract).write(df)
UpsertWriter(contract).write(df, merge_keys=["id"])
PartitionWriter(contract).write(df, partition={"fecha": "2024-01-15"})
DeleteWriter(contract).delete("fecha < '2023-01-01'", preview=True)
```
