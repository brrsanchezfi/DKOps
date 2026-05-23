# Demo 2 — Manufactura

Pipeline de referencia para construir pipelines **testables y gobernados** con DKOps. Dominio: empresa de manufactura de artículos de aseo (jabones, detergentes, shampoos) con pipeline completo Landing → Bronze → Silver → Gold.

Demuestra dos prácticas de ingeniería que distinguen un pipeline profesional de un script:

1. **`IngestionEngine`** — ingesta declarativa con 3 estrategias Silver diferentes (CSV landing).
2. **Motor DQ declarativo** — reglas tipo `not_null`, `unique`, `range`, `expression` con severidades `error` (bloquea) y `warning` (solo logea).
3. **Funciones puras de transformación** — testeables con `pytest` sin Spark de producción.

```bash
python demos/demo_2/pipeline.py
```

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| `IngestionEngine` batch (CSV) | `pipeline.py` — fases Landing → Bronze |
| Estrategia `incremental_replace` | `lotes_produccion_current` |
| Estrategia `cdc_merge` (I/U/D) | `ordenes_manufactura_current` |
| Estrategia `full_merge` | `ventas_manufactura_current` |
| Motor DQ declarativo | `dq/dq_engine.py` + `dq/rules.py` |
| DQ con severidades `error` / `warning` | Validación post-Silver |
| Funciones puras de transformación | `transformations/` |
| Unit tests con `pytest` (sin Delta) | `tests/` (~35 tests) |
| Datos sucios a propósito | Casing, dups, nulos, pH fuera de rango |

---

## Estructura

```
demo_2/
├── pipeline.py                   # orquestador end-to-end
├── config/
│   └── config.json
├── datagen/
│   ├── main.py
│   ├── generate_lotes.py         # 300 lotes de producción (CSV, datos sucios)
│   ├── generate_ordenes.py       # 200 órdenes CDC (CSV)
│   └── generate_ventas.py        # 400 ventas manufactura (CSV)
├── ingestion/
│   ├── batch/                    # lotes.json, ordenes.json, ventas.json
│   └── silver/                   # lotes_current.json, ordenes_current.json, ventas_current.json
├── tables/
│   ├── bronze_new/               # contratos Bronze
│   ├── silver_new/               # contratos Silver
│   └── gold/                     # contratos Gold
├── transformations/
│   ├── bronze_to_silver.py       # funciones puras: limpieza, dedupe, normalización
│   └── silver_to_gold.py         # funciones puras: KPIs y agregaciones
├── dq/
│   ├── dq_engine.py              # Rule, RuleSet, DQReport
│   └── rules.py                  # reglas declarativas por tabla
└── tests/
    ├── conftest.py               # fixture SparkSession (scope=session)
    ├── test_bronze_to_silver.py
    ├── test_silver_to_gold.py
    └── test_dq_engine.py
```

---

## Modelo de datos

```
        BRONZE (raw, sucios)            SILVER (limpio, tipado)       GOLD (KPIs)
┌───────────────────────────┐    ┌───────────────────────────┐    ┌──────────────────────┐
│  lotes_produccion_raw     │ ─► │  lotes_produccion_current │ ─► │  eficiencia_planta   │
│  (QC vacío, pH out-range) │    │  (QC normalizado, merma)  │    │  (diaria por línea)  │
├───────────────────────────┤    ├───────────────────────────┤    ├──────────────────────┤
│  ordenes_manufactura_raw  │ ─► │  ordenes_manufactura_current    │  calidad_lotes       │
│  (estados sucios, dups)   │    │  (estado normalizado, dedup)│    │  (mensual/producto)  │
├───────────────────────────┤    ├───────────────────────────┤    └──────────────────────┘
│  ventas_manufactura_raw   │ ─► │  ventas_manufactura_current│
│  (CANCELLED, devol.)      │    │  (solo CONFIRMED+RETURNED) │
└───────────────────────────┘    └───────────────────────────┘
```

---

## Cómo ejecutarlo

### Pipeline completo

```bash
# Desde la raíz del repositorio
python demos/demo_2/pipeline.py
```

Ejecuta en orden: Landing → Bronze → Silver (con DQ) → Gold (con DQ) → reporte.

Si alguna regla DQ con severidad `error` falla, el pipeline aborta. Los `warning` solo aparecen en el reporte.

### Unit tests

```bash
cd demos/demo_2
pytest tests/ -v                              # todos los tests
pytest tests/test_bronze_to_silver.py -v     # solo una capa
pytest tests/ -v -k "ranking"                # por nombre
```

~35 tests contra DataFrames en memoria — sin Delta, sin catálogo. La SparkSession se comparte entre todos los tests (fixture `scope=session`) para minimizar el tiempo de arranque.

---

## Motor DQ declarativo

```python
# Ejemplo de reglas declarativas
REGLAS_LOTES = [
    {"type": "not_null",   "columns": ["lote_id", "linea_produccion"]},
    {"type": "unique",     "columns": ["lote_id"]},
    {"type": "in_set",     "column": "estado_qc", "allowed": ["APROBADO", "RECHAZADO", "PENDIENTE"]},
    {"type": "range",      "column": "ph_valor", "min": 5.0, "max": 9.0, "severity": "warning"},
    {"type": "expression", "name":  "merma_no_negativa",
     "expression": "merma_kg IS NULL OR merma_kg >= 0"},
]
```

Salida de reporte:

```
DQ Report — silver.manufactura_new.lotes_produccion_current (5 regla(s)):
──────────────────────────────────────────────────────────────────────
  ✔ [error  ] not_null(lote_id, ...)        failed=    0/300 (  0.0%)
  ✔ [error  ] unique(lote_id)               failed=    0/300 (  0.0%)
  ✔ [error  ] in_set(estado_qc ∈ [...])    failed=    0/300 (  0.0%)
  ⚠ [warning] range(ph_valor ∈ [5.0, 9.0]) failed=    4/300 (  1.3%)
  ✔ [error  ] merma_no_negativa             failed=    0/300 (  0.0%)
──────────────────────────────────────────────────────────────────────
Status: PASSED (con warnings) | errors=0 | warnings=1
```

---

## Próximo paso

Demo 3 agrega streaming (Structured Streaming + `availableNow`), enmascaramiento de columnas (`mask`) y schema evolution (`merge_schema`). Ver [demo_3/README.md](../demo_3/README.md).
