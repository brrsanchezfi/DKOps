# Registro operativo

`IngestionOpsLogger` guarda en una tabla Delta el ciclo de vida de cada dataset que se
ingiere o se promueve. A diferencia del log de aplicación, esta información persiste y se
consulta con SQL.

## Activarlo

Basta con indicar `ops_path` al crear el engine:

```python
engine = IngestionEngine.from_launcher(
    bronze_contracts_dir = "ingestion/batch",
    silver_contracts_dir = "ingestion/silver",
    tables_base_dir      = ".",
    ops_path             = "/mnt/datalake/_ops/ingestas",
)
```

Desde ese momento `ingest_bronze()` y `promote_silver()` registran cada dataset sin que
tengas que hacer nada más. Si omites `ops_path`, el registro queda desactivado y el
pipeline funciona igual.

## Esquema de la tabla

| Columna | Tipo | Notas |
|---|---|---|
| `run_id` | `STRING` | Identificador corto de la ejecución |
| `pipeline` | `STRING` | Nombre del pipeline |
| `dataset` | `STRING` | Dataset ingerido o promovido |
| `status` | `STRING` | `STARTED`, `SUCCESS` o `FAILED` |
| `rows_read` | `LONG` | |
| `rows_written` | `LONG` | |
| `started_at` | `TIMESTAMP` | |
| `finished_at` | `TIMESTAMP` | Solo en las filas de cierre |
| `notes` | `STRING` | Detalles, o la traza si falló |

!!! warning "Es un registro de eventos, no una tabla de estado"

    Cada ejecución deja **dos filas**: una `STARTED` al empezar y otra `SUCCESS` o
    `FAILED` al terminar. Toda agregación debe filtrar por `status`, o contará cada
    ejecución dos veces.

    Las dos filas llevan el mismo `started_at`, así que la duración se calcula sobre la
    fila de cierre sin necesidad de unir la tabla consigo misma.

## Consultarla

```python
ops = engine.ops.read()
ops.orderBy("started_at", ascending=False).show(20, truncate=False)
```

Para consultas SQL de monitoreo, ve a [Consultas de monitoreo](ops-queries.md).

## Usarlo fuera del engine

También puedes registrar tus propios procesos. El ciclo es explícito: `log_start()`
devuelve el `run_id` que necesitan los cierres.

```python
from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger

ops    = IngestionOpsLogger(spark, ops_path="/mnt/_ops/mi_proceso",
                            pipeline="carga_manual")
run_id = ops.log_start("ventas")

try:
    filas = mi_carga()
    ops.log_success(run_id, "ventas", rows_written=filas)
except Exception as exc:
    ops.log_failure(run_id, "ventas", exc)
    raise
```

Presta atención al `raise`. `log_failure()` deja constancia del error pero no lo
propaga; si no lo relanzas, el proceso termina en verde con un `FAILED` en la tabla.

## Garantías y límites

**El registro nunca detiene el pipeline.** Si falla la escritura en la tabla de control,
se registra un `ERROR` en el log de aplicación y la ejecución continúa. Detener una
ingesta que fue bien solo porque no se pudo anotar su cierre sería peor que el problema.

Por eso conviene vigilar los mensajes `ERROR` que empiezan por `OpsLogger:`. Indican que
la tabla se está quedando incompleta. Hasta la v0.3.3 se registraban como `WARNING`, y así
pasó desapercibido que no se escribía ninguna fila de cierre.

**Si el proceso se reinicia entre el inicio y el cierre**, la fila de cierre se escribe
igualmente, pero con `started_at` a `NULL` porque ese valor se guardaba en memoria. Para
calcular su duración hay que unir por `run_id` con la fila `STARTED`. Puedes localizarlas
así:

```sql
SELECT * FROM ops WHERE status <> 'STARTED' AND started_at IS NULL
```
