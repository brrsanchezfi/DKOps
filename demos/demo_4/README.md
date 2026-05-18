# Demo 4 — TableReader: lectura gobernada y Change Data Feed

Dominio **tienda** (inventario de productos + ventas diarias). Demuestra las cuatro capacidades de `TableReader`.

## Qué aprenderás

### `read()` — Lectura completa con opciones

```python
reader = TableReader(contract)

df = reader.read()                                        # tabla completa
df = reader.read(filter="categoria = 'ELECTRONICA'")     # con predicado SQL
df = reader.read(columns=["id", "stock"])                # proyección
df = reader.read(limit=10)                               # top N

# El resultado es un DataFrame real — se puede seguir transformando
reader.read().groupBy("categoria").sum("stock").show()
```

### `read_partition()` — Lectura eficiente por partición

```python
df = reader.read_partition({"fecha": "2024-01-15"})
```

Valida que la columna sea de partición antes de consultar. Lanza `ValueError` con mensaje claro si no lo es.

### `read_stream()` — Structured Streaming

```python
stream = reader.read_stream()   # isStreaming == True

query = (
    stream.writeStream
    .foreachBatch(mi_funcion)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
```

### `read_cdf()` — Change Data Feed

Requiere `"change_data_feed": true` en el contrato. El `CreateWriter` activa `delta.enableChangeDataFeed` en `TBLPROPERTIES` automáticamente.

```json
{
  "change_data_feed": true,
  "columns": [...]
}
```

```python
# Por número de versión
df = reader.read_cdf(starting_version=1)

# Por timestamp
df = reader.read_cdf(starting_timestamp="2024-01-15T00:00:00")

# Rango de versiones
df = reader.read_cdf(starting_version=1, ending_version=5)
```

El DataFrame del CDF incluye columnas adicionales:
- `_change_type`: `insert`, `update_preimage`, `update_postimage`, `delete`
- `_commit_version`: número de versión Delta del cambio
- `_commit_timestamp`: timestamp del commit

## Estructura

```
demo_4/
├── config/
│   └── config.json              # configuración local
├── tables/
│   ├── inventario.json          # change_data_feed: true + particiones por categoria
│   └── ventas_diarias.json      # sin CDF, particionada por fecha
├── data_generator.py            # inventario inicial + actualizaciones + ventas
├── pipeline_lectura.py          # pipeline principal (6 fases)
└── README.md
```

## Ejecutar

```bash
source ../../.venv-local/bin/activate   # Linux / Mac / WSL

cd demos/demo_4
python pipeline_lectura.py
```

## Flujo del demo

| Fase | Operación | Feature |
|---|---|---|
| 1 | Bootstrap del inventario y ventas | `TableWriter.overwrite/partition` |
| 2 | Lectura completa, filtrada, con proyección | `read()` |
| 3 | Lectura de partición de ventas | `read_partition()` |
| 4 | Upsert de stock → lectura del CDF | `read_cdf()` |
| 5 | Streaming con `foreachBatch` | `read_stream()` |
| 6 | Resumen | — |
