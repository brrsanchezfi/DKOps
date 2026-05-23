# Demo 4 — Retail / Inventario

Pipeline Lakehouse completo para gestión de **inventario retail**. Demuestra las capacidades avanzadas de `TableReader`: Change Data Feed, streaming gobernado, y planificación de migraciones con `SafeMigrator`.

```bash
python demos/demo_4/pipeline.py
```

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| `IngestionEngine` batch + streaming | `pipeline.py` — fases Landing → Bronze |
| `full_merge` — catálogo de productos | `productos_current` (36 SKUs) |
| `append_dedup` — movimientos de stock | `movimientos_current` (200 mov.) |
| `append_dedup` — alertas IoT | `alertas_current` (60 alertas) |
| `TableReader.read_cdf()` | Gold: detecta cambios de stock via Change Data Feed |
| `TableReader.read_stream()` | Validación: streaming gobernado sobre Silver |
| `SafeMigrator(dry_run=True)` | Fase 5: plan de migración sin ejecutar |
| Streaming con `availableNow` | `alertas` vía `run_streaming()` |

---

## Estructura

```
demo_4/
├── pipeline.py                  # orquestador — 6 fases
├── config/
│   └── config.json
├── datagen/
│   ├── main.py
│   ├── generate_productos.py    # 36 SKUs (CSV)
│   ├── generate_movimientos.py  # 200 movimientos (JSON)
│   └── generate_alertas.py      # 60 alertas IoT (JSON)
├── ingestion/
│   ├── batch/                   # productos.json, movimientos.json
│   ├── streaming/               # alertas.json
│   └── silver/                  # productos_current.json, movimientos_current.json, alertas_current.json
└── tables/
    ├── bronze/
    ├── silver/
    └── gold/                    # stock_actual.json, alertas_criticas.json
```

---

## Flujo Landing → Bronze → Silver → Gold

```
Landing                    Bronze                  Silver                Gold
────────────────────────────────────────────────────────────────────────────────────
productos/ (36 CSV)    →   productos_raw       →   productos_current  →
                           (full snapshot)         (full_merge)           stock_actual
movimientos/ (200)     →   movimientos_raw     →   movimientos_current    (via CDF)
                           (incremental)           (append_dedup)
                                                                       →  alertas_criticas
alertas/ (60) [STREAM] →   alertas_raw         →   alertas_current        (41 alertas)
                           (streaming→batch)        (append_dedup)
```

---

## Change Data Feed

`productos_current` tiene `"change_data_feed": true` en su contrato. Tras las actualizaciones de stock, Gold calcula el stock actual detectando solo los cambios:

```python
reader = TableReader(ct_productos_current)

# Lee cambios desde la versión 1 del Delta log
df_cambios = reader.read_cdf(starting_version=1)

# _change_type: "update_postimage" contiene el nuevo valor
df_stock = (
    df_cambios
    .filter("_change_type = 'update_postimage'")
    .select("producto_id", "stock", "_commit_version")
)
```

---

## Streaming gobernado

```python
reader = TableReader(ct_movimientos_current)

stream_df = reader.read_stream()   # isStreaming == True

query = (
    stream_df
    .groupBy("categoria")
    .count()
    .writeStream
    .outputMode("complete")
    .format("console")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
```

---

## SafeMigrator

```python
from DKOps.table_governance import SafeMigrator

# Solo planifica — no ejecuta nada
plan = SafeMigrator(ct_productos_current, dry_run=True).apply()
```

Compara el contrato JSON contra el estado real de la tabla en Delta y genera el `ALTER TABLE` mínimo necesario (añadir columnas, actualizar comentarios, cambiar propiedades). Nunca elimina columnas.

---

## Fases del pipeline

| Fase | Operación | Feature clave |
|---|---|---|
| 0 | Genera datos en Landing | CSV productos + JSON movimientos + JSON alertas |
| 1 | Inicializa DKOps | `Launcher` + `IngestionEngine` |
| 2 | Landing → Bronze | `ingest_bronze()` + `run_streaming()` |
| 3 | Bronze → Silver | `promote_silver()` — 3 estrategias |
| 4 | Silver → Gold | `read_cdf()` + SQL agregaciones alertas |
| 5 | Validación | `read_stream()` + `SafeMigrator` dry_run |

---

## Cómo ejecutarlo

```bash
# Desde la raíz del repositorio
python demos/demo_4/pipeline.py
```

El demo es idempotente — puede ejecutarse múltiples veces sin limpiar nada.

---

## Próximo paso

Demo 5 combina `cdc_merge` + `full_merge` + streaming en un dominio marketplace, produciendo métricas de revenue y engagement en Gold. Ver [demo_5/README.md](../demo_5/README.md).
