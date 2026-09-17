# Escritura

`TableWriter` es la forma recomendada de escribir en una tabla gobernada. Valida el
DataFrame contra el contrato, elige el writer interno que corresponde a cada operación y
funciona igual en local y en Databricks.

```python
from DKOps.table_governance import load_contract, TableWriter

contract = load_contract("tables/silver/fact_ventas.json")
writer   = TableWriter(contract)

writer.overwrite(df)
writer.append(df)
writer.upsert(df, keys=["venta_id", "fecha"])
writer.overwrite_partition(df, partition={"fecha": "2024-01-15"})
writer.delete("fecha < '2023-01-01'")
writer.apply_contract_metadata()
```

## Operaciones

| Método | SQL equivalente | Uso típico |
|---|---|---|
| `overwrite(df)` | `CREATE OR REPLACE TABLE` | Carga completa o creación inicial |
| `append(df)` | `INSERT INTO` | Añadir filas sin tocar las existentes |
| `upsert(df, keys)` | `MERGE INTO` | Actualizar e insertar por clave |
| `overwrite_partition(df, partition)` | Sobrescritura de una partición | Reprocesar un día concreto |
| `delete(condition)` | `DELETE FROM ... WHERE` | Borrar filas por condición |
| `apply_contract_metadata()` | `COMMENT`, `SET MASK`, `GRANT` | Aplicar la metadata sin reescribir datos |

## Opciones del constructor

| Parámetro | Tipo | Por defecto | Descripción |
|---|---|---|---|
| `contract` | `TableContract` | | Contrato cargado con `load_contract()` |
| `strict_columns` | `bool` | `True` | Falla si el DataFrame trae columnas que el contrato no declara |
| `fail_on_warning` | `bool` | `False` | Trata las advertencias del validador como errores |
| `dry_run` | `bool` | `False` | Simula la operación sin escribir nada |

```python
# Ver qué haría sin escribir
TableWriter(contract, dry_run=True).overwrite(df)

# Ser estricto también con las advertencias
TableWriter(contract, fail_on_warning=True).append(df)
```

## upsert

```python
writer.upsert(
    df,
    keys=["id", "fecha"],          # columnas del join, obligatorio
    update_columns=["estado"],     # si se omite, se actualizan todas
)
```

### Columnas que solo se insertan

Algunas columnas solo tienen sentido en el momento de la inserción. Si el `UPDATE` las
sobrescribiera en cada ejecución, perderían su significado. El ejemplo típico es una
fecha de creación.

```python
writer.upsert(
    df,
    keys=["venta_id"],
    insert_only_columns=["_silver_created_at"],
)
```

La columna se incluye en `WHEN NOT MATCHED THEN INSERT` y se excluye de
`WHEN MATCHED THEN UPDATE SET`. Si al excluirla no quedara ninguna columna que actualizar,
el writer lanza `ValueError` en vez de generar un MERGE inútil.

Las estrategias de promoción a Silver ya lo hacen con `_silver_created_at`. Ver
[Columnas técnicas](../ingestion/metadata.md#en-silver).

## overwrite_partition

Reemplaza solo la partición indicada y deja intacto el resto de la tabla:

```python
writer.overwrite_partition(df_dia5, partition={"fecha": "2024-01-05"})
```

Las claves del diccionario deben ser columnas de partición del contrato.

## delete

```python
filas = writer.delete("fecha < '2023-01-01'")
```

Devuelve el número de filas borradas. La condición no puede estar vacía; para vaciar una
tabla usa `overwrite` con un DataFrame vacío.

Con `preview=True`, antes de borrar se muestran en el log las filas afectadas:

```python
writer.delete("estado = 'CANCELLED'", preview=True)
```

!!! warning "preview no evita el borrado"

    `preview=True` enseña las filas y después las borra. Si solo quieres ver cuántas
    filas se eliminarían, combínalo con `dry_run=True`:

    ```python
    TableWriter(contract, dry_run=True).delete("estado = 'CANCELLED'", preview=True)
    ```

## Writers individuales

`TableWriter` delega en writers especializados. Siguen disponibles para casos avanzados,
aunque en general no los necesitarás:

```python
from DKOps.table_governance import (
    CreateWriter, AppendWriter, UpsertWriter, PartitionWriter, DeleteWriter,
)

CreateWriter(contract).write(df)
AppendWriter(contract).write(df)
UpsertWriter(contract).write(df, merge_keys=["id"])
PartitionWriter(contract).write(df, partition={"fecha": "2024-01-15"})
DeleteWriter(contract).delete("fecha < '2023-01-01'")
```
