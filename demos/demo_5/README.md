# Demo 5 — Marketplace

Pipeline Lakehouse completo para un **marketplace e-commerce**. Combina ventas con CDC (I/U/D), catálogo de clientes con full merge y eventos de app en streaming, produciendo métricas de revenue y engagement en Gold.

```bash
python demos/demo_5/pipeline.py
```

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| `cdc_merge` — ventas CDC con soft delete | `ventas_current` — `op_type` → `is_deleted` |
| `full_merge` — catálogo de clientes | `clientes_current` — snapshot diario |
| Streaming con `availableNow` | `eventos_app` — `run_streaming()` |
| Gold: revenue por canal | `revenue_diario` — SUM/AVG/COUNT por canal |
| Gold: engagement por cliente | `engagement_clientes` — revenue + ventas por cliente |
| Tabla de control operativo | `engine.ops.read()` — auditoría por dataset |
| Idempotencia completa | Partition overwrite Bronze + upsert Silver |

---

## Estructura

```
demo_5/
├── pipeline.py                  # orquestador — 6 fases
├── config/
│   └── config.json
├── datagen/
│   ├── main.py
│   ├── generate_ventas.py       # 400 ventas CDC (I/U/D)
│   ├── generate_clientes.py     # 300 clientes full snapshot
│   └── generate_eventos.py      # 200 eventos app
├── ingestion/
│   ├── batch/                   # ventas_diarias.json, clientes.json
│   ├── streaming/               # eventos_app.json
│   └── silver/                  # ventas_current.json, clientes_current.json
└── tables/
    ├── bronze/                  # ventas_raw.json, clientes_raw.json
    ├── silver/                  # ventas_current.json, clientes_current.json
    └── gold/                    # revenue_diario.json, engagement_clientes.json
```

---

## Flujo Landing → Bronze → Silver → Gold

```
Landing                  Bronze                   Silver               Gold
──────────────────────────────────────────────────────────────────────────────────
ventas_diarias/ (400) →  ventas_raw           →   ventas_current    →
CDC: I/U/D               (_ingested_at            (cdc_merge         revenue_diario
                          _ingested_date           is_deleted)        (3 filas / canal)
                          _source_file)

clientes/ (300)       →  clientes_raw         →   clientes_current  → engagement_clientes
full snapshot            (full snapshot diario)    (full_merge        (165 filas)
                                                   150 activos)

eventos_app/ (200)    →  [Bronze sin Silver]
[STREAMING]
```

---

## cdc_merge — Soft delete

Las ventas llegan con `op_type: I | U | D` desde el sistema origen. La estrategia `cdc_merge` las procesa así:

- `I` o `U` → MERGE INTO Silver con `is_deleted = False`
- `D` → MERGE INTO Silver con `is_deleted = True` (soft delete)

Esto permite conservar el historial de ventas canceladas en Silver sin perder trazabilidad.

```json
{
  "strategy":    "cdc_merge",
  "merge_keys":  ["venta_id"],
  "watermark_col": "fecha_venta",
  "metadata": { "add_silver_timestamps": true }
}
```

---

## Gold: revenue y engagement

```sql
-- revenue_diario: métricas por canal
SELECT
    canal,
    COUNT(*)                                               AS total_ventas,
    SUM(precio_total)                                      AS revenue_total,
    AVG(precio_total)                                      AS revenue_promedio,
    SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) AS ventas_canceladas
FROM silver.ecommerce.ventas_current
WHERE canal IS NOT NULL
GROUP BY canal;

-- engagement_clientes: revenue y actividad por cliente
SELECT
    v.cliente_id,
    COUNT(v.venta_id)     AS total_ventas,
    SUM(v.precio_total)   AS revenue_cliente,
    FIRST(v.canal)        AS canal_preferido
FROM silver.ecommerce.ventas_current v
WHERE v.cliente_id IS NOT NULL
  AND (v.is_deleted IS NULL OR NOT v.is_deleted)
GROUP BY v.cliente_id;
```

---

## Tabla de control operativo

```python
ops_df = engine.ops.read()
ops_df.select("run_id", "dataset", "status", "rows_written", "started_at").show()
```

Cada ejecución registra automáticamente dataset, filas escritas, estado y timestamps para auditoría.

---

## Cómo ejecutarlo

```bash
# Desde la raíz del repositorio
python demos/demo_5/pipeline.py
```

El pipeline es completamente idempotente:
- **Bronze**: partition overwrite por `_ingested_date`
- **Silver**: MERGE INTO (no crea duplicados)
- **Streaming**: checkpoints en `/tmp/dkops_demo5/checkpoints/`
- **Gold**: `overwrite` — reemplaza en cada ejecución
