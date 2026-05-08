# Demo 2 — Transformaciones testeables y Data Quality declarativo

Demo de referencia para construir pipelines **testables y gobernados** con DKOps. Toma como dominio una empresa de manufactura de artículos de aseo (jabones, detergentes, shampoos) con un pipeline en tres capas (bronze → silver → gold) y demuestra dos prácticas de ingeniería que distinguen un pipeline profesional de un script:

1. **Funciones puras de transformación** que se pueden testear con `pytest` sin Spark de producción ni Delta — solo DataFrames en memoria.
2. **Data Quality checks declarativos** ejecutados después de cada escritura — reglas tipo "no nulls", "unicidad", "rango", "expresión SQL", con severidades `error` (bloquea) y `warning` (solo logea).

Si demo-1 mostró *cómo escribir tablas gobernadas*, demo-2 muestra *cómo confiar en lo que escribes*.

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| Pipeline en 3 capas (bronze → silver → gold) | `pipeline_manufactura.py` |
| Datos sintéticos *intencionalmente sucios* en bronze | `data_generator.py` |
| Funciones puras de transformación (sin side effects) | `transformations/` |
| Unit tests con `pytest` sobre funciones de transformación | `tests/test_*.py` |
| Fixture compartida de SparkSession (scope=session) | `tests/conftest.py` |
| Motor DQ declarativo (`Rule`, `RuleSet`, `DQReport`) | `dq/dq_engine.py` |
| Reglas DQ por tabla (NotNull, Unique, InSet, Range, Expression) | `dq/rules.py` |
| DQ con severidades `error` / `warning` | `dq/dq_engine.py` |
| Integración DQ en el pipeline (post-write check) | `pipeline_manufactura.py` |

---

## Estructura

```
demo-2/
├── pipeline_manufactura.py         # orquestador end-to-end
├── data_generator.py                # genera datos bronze sucios
├── pytest.ini                       # config de tests
├── config/
│   └── config.json                  # config del Launcher
├── tables/                          # contratos de las 9 tablas (3 bronze + 3 silver + 3 gold)
│   ├── bronze_*.json
│   ├── silver_*.json
│   └── gold_*.json
├── transformations/                 # funciones puras
│   ├── bronze_to_silver.py          #   limpieza, dedupe, normalización
│   └── silver_to_gold.py            #   KPIs y agregaciones
├── dq/                              # motor de Data Quality
│   ├── dq_engine.py                 #   Rule, RuleSet, DQReport
│   └── rules.py                     #   reglas declarativas por tabla
└── tests/                           # unit tests con pytest
    ├── conftest.py                  #   fixture spark + helpers
    ├── test_bronze_to_silver.py
    ├── test_silver_to_gold.py
    └── test_dq_engine.py
```

---

## Modelo de datos

```
            BRONZE (raw, sucio)              SILVER (limpio, tipado)         GOLD (KPIs)
   ┌──────────────────────────────┐    ┌──────────────────────────────┐    ┌──────────────────────────┐
   │  ordenes_produccion_raw      │ ─► │  ordenes_produccion          │ ─► │  eficiencia_planta       │
   │  (estados sucios, dups)      │    │  (estado normalizado, dedup) │    │  (diaria por línea)      │
   ├──────────────────────────────┤    ├──────────────────────────────┤    ├──────────────────────────┤
   │  lotes_produccion_raw        │ ─► │  lotes_produccion            │ ─► │  calidad_lotes           │
   │  (QC vacío, pH out-of-range) │    │  (QC normalizado, merma)     │    │  (mensual por producto)  │
   ├──────────────────────────────┤    ├──────────────────────────────┤    ├──────────────────────────┤
   │  ventas_raw                  │ ─► │  ventas                      │ ─► │  ventas_producto         │
   │  (CANCELLED, devoluciones)   │    │  (solo CONFIRMED + RETURNED) │    │  (mensual con ranking)   │
   └──────────────────────────────┘    └──────────────────────────────┘    └──────────────────────────┘
```

---

## Cómo correrlo

### 1. Pipeline end-to-end

```bash
cd demo-2
python3 pipeline_manufactura.py
```

Ejecuta las 4 fases:
1. **Bootstrap bronze** — genera datos sucios y los escribe.
2. **Bronze → Silver** — aplica las funciones de transformación + DQ check post-escritura.
3. **Silver → Gold** — calcula los 3 KPIs + DQ check.
4. **Reporte** — queries de negocio sobre las 3 capas.

Si alguna regla DQ con severidad `error` falla, el pipeline aborta. Los warnings solo aparecen en el reporte sin bloquear.

### 2. Unit tests

```bash
cd demo-2
pytest tests/ -v
```

Aproximadamente 35 tests — todos contra DataFrames en memoria, sin Delta ni Unity Catalog. La SparkSession se crea una sola vez y se comparte (fixture `scope=session`), así que el tiempo total ronda los 5–10 segundos tras el arranque inicial de Spark (~20 s la primera vez).

Para correr solo un subconjunto:

```bash
pytest tests/test_bronze_to_silver.py -v             # solo bronze→silver
pytest tests/ -v -k "TestKpiVentas"                  # solo una clase
pytest tests/ -v -k "ranking"                        # solo tests con "ranking" en el nombre
```

---

## Prácticas demostradas

### 1. Transformaciones puras = testeables

Cada función en `transformations/` recibe `DataFrame(s)` y devuelve `DataFrame`. **No accede a Spark global, no escribe, no lee del catálogo**. Esto las hace triviales de testear:

```python
def test_normaliza_estado(spark):
    df = spark.createDataFrame([("completed",), ("OK",)],
                                T.StructType([T.StructField("estado", T.StringType())]))
    result = normalizar_estado_orden(df).collect()
    assert all(r.estado == "COMPLETED" for r in result)
```

El pipeline orquestador es el que se encarga de leer/escribir vía writers; las funciones de transformación nunca tocan I/O.

### 2. Datos sucios *a propósito*

`data_generator.py` introduce intencionalmente:
- Casing inconsistente (`"completed"`, `"Complete"`, `"OK"`, `"FINALIZADA"`)
- Duplicados (~5%)
- Nulos en columnas opcionales (operador)
- Estados inválidos (`"???"`, `""`, `None`)
- Devoluciones con cantidad negativa
- pH fuera de rango ocasional

Esto es importante porque demuestra que las transformaciones funcionan con **datos realistas**, no con datasets idealizados.

### 3. Fixture de SparkSession con scope=session

En `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    ...
```

Crear una `SparkSession` toma 5–10 segundos. Compartirla entre todos los tests reduce el tiempo total dramáticamente. Cada test recibe la fixture vía argumento (`def test_x(spark): ...`).

### 4. DQ checks declarativos

Las reglas se escriben como dicts (fácilmente exportables a YAML/JSON):

```python
{"type": "not_null",  "columns": ["orden_id"]}
{"type": "unique",    "columns": ["orden_id"]}
{"type": "in_set",    "column": "estado", "allowed": ["COMPLETED", "IN_PROGRESS", "CANCELLED"]}
{"type": "range",     "column": "cumplimiento_pct", "min": 0, "max": 200, "severity": "warning"}
{"type": "expression","name": "completed_tiene_fecha_fin",
 "expression": "estado != 'COMPLETED' OR fecha_fin IS NOT NULL"}
```

El motor (`dq/dq_engine.py`) las ejecuta y devuelve un `DQReport` con los resultados. `report.raise_if_failed()` lanza solo si hay errores; los warnings se imprimen pero no bloquean.

### 5. Salida visible y accionable

Cada DQ check imprime un reporte legible:

```
  DQ Report — silver.manufactura.ordenes_produccion (8 regla(s)):
  ────────────────────────────────────────────────────────────────────────────────
    ✔ [error  ] not_null(orden_id, linea_id, ...)               failed=    0/187 (  0.0%)
    ✔ [error  ] unique(orden_id)                                failed=    0/187 (  0.0%)
    ✔ [error  ] in_set(estado ∈ [...])                          failed=    0/187 (  0.0%)
    ⚠ [warning] range(cumplimiento_pct ∈ [0, 200])              failed=    3/187 (  1.6%)
    ...
  ────────────────────────────────────────────────────────────────────────────────
  Status: PASSED (con warnings) | errors=0 | warnings=1
```

---

## ¿Por qué este módulo DQ no está en DKOps?

Está intencionalmente como módulo **local del demo** para validar la idea antes de cementarla en el framework. Si demuestra valor en proyectos reales, el siguiente paso natural es promoverlo a `DKOps.data_quality` y permitir que las reglas vivan dentro del JSON del contrato:

```json
{
  "name": "ordenes_produccion",
  "columns": [...],
  "data_quality": [
    {"type": "not_null", "columns": ["orden_id"]},
    ...
  ]
}
```

Los writers podrían entonces ejecutar las reglas automáticamente tras cada escritura sin que el pipeline lo pida explícitamente.

---

## Próximos demos

- **demo-1** — Contratos y writers gobernados (referencia base).
- **demo-2** — Transformaciones testeables + DQ declarativo (este).
- **demo-3** — *(por definir)*

Cada demo construye sobre el anterior. Si vienes de demo-1, este es el siguiente paso lógico.