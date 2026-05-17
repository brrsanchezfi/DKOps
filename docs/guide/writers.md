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

### delete

```python
rows_deleted = writer.delete("fecha < '2023-01-01'")
rows_preview = writer.delete("estado = 'CANCELLED'", preview=True)  # no borra, solo cuenta
```

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
