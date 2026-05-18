# Readers

`TableReader` es la fachada de lectura gobernada por contrato. Devuelve siempre un `DataFrame` nativo de PySpark — todos los métodos de transformación (`.filter`, `.groupBy`, `.join`, etc.) funcionan sin ningún proxy.

## Creación

```python
from DKOps.table_governance import load_contract, TableReader

contract = load_contract("tables/inventario.json")
reader   = TableReader(contract)
```

`TableReader` obtiene `SparkSession` y `EnvironmentConfig` del `Launcher` activo, igual que los writers. No hay parámetros adicionales.

---

## read() — Lectura completa

```python
# Tabla completa
df = reader.read()

# Con filtro SQL
df = reader.read(filter="categoria = 'ELECTRONICA' AND activo = true")

# Solo ciertas columnas
df = reader.read(columns=["producto_id", "nombre", "stock"])

# Top N filas
df = reader.read(limit=10)

# Combinados
df = reader.read(
    filter="activo = true",
    columns=["producto_id", "nombre", "precio_usd"],
    limit=5,
)
```

El resultado es un `DataFrame` real — se puede seguir transformando con cualquier API de PySpark:

```python
resumen = (
    reader.read()
    .groupBy("categoria")
    .agg({"stock": "sum", "precio_usd": "avg"})
    .orderBy("categoria")
)
resumen.show()
```

### Validaciones

- `columns` — lanza `ValueError` si alguna columna no existe en el DataFrame leído.
- `limit` — lanza `ValueError` si el valor es ≤ 0.

---

## read_partition() — Lectura por partición

Lee solo una partición concreta de forma eficiente. Valida que la columna sea de partición antes de consultar.

```python
# Tabla particionada por fecha
df = reader.read_partition({"fecha": "2024-01-15"})

# Tabla particionada por categoria
df = reader.read_partition({"categoria": "ELECTRONICA"})

# Varias columnas de partición
df = reader.read_partition({"anio": "2024", "mes": "01"})
```

Si se pasa una columna que no es de partición, lanza `ValueError` con mensaje claro:

```python
try:
    reader.read_partition({"producto_id": "P001"})
except ValueError as e:
    print(e)
# ValueError: 'producto_id' no es columna de partición.
# Particiones definidas en el contrato: ['fecha']
```

---

## read_stream() — Structured Streaming

Lee la tabla como un `DataFrame` de Structured Streaming usando el log Delta.

```python
stream_df = reader.read_stream()
print(stream_df.isStreaming)   # True

query = (
    stream_df.writeStream
    .foreachBatch(mi_funcion)
    .trigger(availableNow=True)   # procesa todo lo disponible y termina
    .start()
)
query.awaitTermination(timeout=30)
```

### Ejemplo con foreachBatch

```python
def procesar_batch(batch_df, batch_id: int) -> None:
    print(f"Batch {batch_id}: {batch_df.count()} filas")
    batch_df.write.format("delta").mode("append").saveAsTable("silver.inventario")

query = (
    reader.read_stream().writeStream
    .foreachBatch(procesar_batch)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
```

---

## read_cdf() — Change Data Feed

Lee el historial de cambios de la tabla. Requiere `"delta.enableChangeDataFeed": "true"` en el contrato.

```python
# Desde una versión específica
df = reader.read_cdf(starting_version=1)

# Desde un timestamp
df = reader.read_cdf(starting_timestamp="2024-01-15T00:00:00")

# Rango de versiones
df = reader.read_cdf(starting_version=1, ending_version=5)
```

El DataFrame incluye columnas adicionales de Delta CDF:

| Columna | Descripción |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Número de versión Delta del commit |
| `_commit_timestamp` | Timestamp del commit |

### Activar CDF en el contrato

```json
{
  "properties": {
    "delta.enableChangeDataFeed": "true"
  }
}
```

### Errores y validaciones

```python
# Sin CDF habilitado → ValueError
TableReader(contrato_sin_cdf).read_cdf(starting_version=0)
# ValueError: La tabla 'tienda.inventario' no tiene change_data_feed habilitado.
# Agrega "delta.enableChangeDataFeed": "true" en properties del contrato.

# Sin punto de inicio → ValueError
reader.read_cdf()
# ValueError: Debes pasar starting_version o starting_timestamp.

# Ambos a la vez → ValueError
reader.read_cdf(starting_version=1, starting_timestamp="2024-01-15T00:00:00")
# ValueError: Pasa solo uno: starting_version o starting_timestamp.
```

---

## Resolución de nombre de tabla

El `TableReader` usa el mismo criterio que los writers:

| Runtime | Nombre efectivo |
|---|---|
| Databricks | `catalog.schema.name` (Unity Catalog) |
| PC local | `schema.name` (catálogo nativo de Spark) |

```python
contract = load_contract("tables/inventario.json")
# En Databricks: lee de  ct_bronze_dev.tienda.inventario
# En local PC:   lee de  tienda.inventario
reader = TableReader(contract)
```

---

## Logging

Todas las operaciones loguean inicio, fin y duración:

```
2024-01-15 10:23:01 | INFO    | TableReader.read     | ▶ read | tabla='tienda.inventario'
2024-01-15 10:23:02 | SUCCESS | TableReader.read     | ✔ read | tabla='tienda.inventario' | elapsed=0.87s | filas=17
2024-01-15 10:23:02 | INFO    | TableReader.read_cdf | ▶ read_cdf | tabla='tienda.inventario' | starting_version=5
2024-01-15 10:23:02 | SUCCESS | TableReader.read_cdf | ✔ read_cdf | tabla='tienda.inventario' | elapsed=0.21s
```
