# Lectura

`TableReader` lee tablas Delta a partir de su contrato. Resuelve el nombre de la tabla
según el runtime y valida los parámetros antes de consultar. Todos sus métodos devuelven
un `DataFrame` normal de PySpark, así que puedes seguir encadenando `filter`, `join` o
`groupBy` como siempre.

```python
from DKOps.table_governance import load_contract, TableReader

contract = load_contract("tables/silver/productos_current.json")
reader   = TableReader(contract)

df = reader.read()
df = reader.read_partition({"categoria": "ROPA"})
df = reader.read_stream()
df = reader.read_cdf(starting_version=5)
```

## read

```python
df = reader.read(
    filter  = "activo = true AND precio > 100",
    columns = ["producto_id", "nombre", "precio"],
    limit   = 1000,
)
```

| Parámetro | Descripción |
|---|---|
| `filter` | Condición SQL, sin la palabra `WHERE` |
| `columns` | Columnas a seleccionar. Si alguna no existe, lanza `ValueError` con la lista de disponibles |
| `limit` | Número máximo de filas |

Los tres son opcionales y equivalen a aplicar `select`, `filter` y `limit` sobre el
resultado.

## read_partition

```python
df = reader.read_partition({"anio": "2024", "mes": "01"})
```

Comprueba que todas las claves sean columnas de partición declaradas en el contrato. Si
el diccionario está vacío o incluye columnas que no son de partición, lanza `ValueError`.

## read_stream

```python
stream = reader.read_stream()

query = (
    stream.writeStream
    .foreachBatch(procesar_lote)
    .trigger(availableNow=True)
    .option("checkpointLocation", "/tmp/checkpoints/productos")
    .start()
)
query.awaitTermination()
```

Devuelve un DataFrame de Structured Streaming que lee incrementalmente el log de Delta.

## Change Data Feed

```python
cambios = reader.read_cdf(starting_version=1)

cambios.select(
    "producto_id", "stock", "_change_type", "_commit_version"
).show()
```

| Parámetro | Descripción |
|---|---|
| `starting_version` | Versión Delta inicial, incluida |
| `starting_timestamp` | Alternativa a la versión: timestamp ISO inicial |
| `ending_version` | Versión final, incluida. Si se omite, se lee hasta la última |

El resultado incluye tres columnas añadidas por Delta:

| Columna | Valores |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Versión Delta del cambio |
| `_commit_timestamp` | Momento del commit |

!!! note "Requisitos"

    El contrato debe tener `"change_data_feed": true` y la tabla tiene que haberse creado
    con esa opción. También es obligatorio indicar `starting_version` o
    `starting_timestamp`. En cualquier otro caso se lanza `ValueError` con un mensaje
    que explica qué falta.
