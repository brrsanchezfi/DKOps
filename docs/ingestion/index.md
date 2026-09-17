# Ingesta

<p class="dk-lead">
El módulo de ingesta mueve los datos desde Landing hasta Silver en dos pasos: primero
los copia a Bronze con columnas de trazabilidad y después los promueve a Silver con la
estrategia que indique cada contrato.
</p>

## IngestionEngine

`IngestionEngine` es el único punto de entrada que necesitas. Lo construyes una vez y
llamas a sus métodos en orden.

=== "Desde el Launcher"

    ```python
    from DKOps.launcher import Launcher
    from DKOps.ingestion.engine import IngestionEngine

    Launcher("config/config.json")

    engine = IngestionEngine.from_launcher(
        bronze_contracts_dir    = "ingestion/batch",
        streaming_contracts_dir = "ingestion/streaming",
        silver_contracts_dir    = "ingestion/silver",
        tables_base_dir         = ".",
        ops_path                = "/tmp/ops/control",
    )
    ```

=== "Con Spark explícito"

    ```python
    launcher = Launcher("config/config.json")

    engine = IngestionEngine.from_spark(
        spark                   = launcher.spark,
        env                     = launcher.env,
        bronze_contracts_dir    = "ingestion/batch",
        streaming_contracts_dir = "ingestion/streaming",
        silver_contracts_dir    = "ingestion/silver",
        tables_base_dir         = ".",
        ops_path                = "/tmp/ops/control",
    )
    ```

```python
engine.ingest_bronze()    # Landing a Bronze, batch
engine.run_streaming()    # Landing a Bronze, streaming
engine.promote_silver()   # Bronze a Silver
engine.status()           # conteo de filas por tabla
```

### Parámetros de construcción

| Parámetro | Descripción |
|---|---|
| `bronze_contracts_dir` | Carpeta con los contratos batch de Landing a Bronze |
| `streaming_contracts_dir` | Carpeta con los contratos streaming |
| `silver_contracts_dir` | Carpeta con los contratos de promoción a Silver |
| `tables_base_dir` | Directorio base del proyecto. Por defecto `.` |
| `ops_path` | Ruta de la tabla de control operativo. Si se omite, no se registra nada |
| `schema_root` | Ruta para los schemas de Auto Loader en Databricks |
| `kafka_creds` | Credenciales de Kafka, ver [Carga streaming](streaming.md#kafka) |

### Métodos

| Método | Qué hace |
|---|---|
| `ingest_bronze(name=None)` | Ejecuta los contratos batch. Devuelve la lista de datasets que fallaron. |
| `run_streaming(name=None)` | Ejecuta los contratos streaming con `availableNow` y espera a que terminen. |
| `start_streaming(name=None)` | Arranca los streams sin bloquear y devuelve las queries. |
| `stop_streaming(queries)` | Detiene las queries que devolvió `start_streaming()`. |
| `promote_silver(name=None)` | Aplica la estrategia de cada contrato Silver. Devuelve la lista de fallos. |
| `status()` | Registra en el log cuántas filas tiene cada tabla Bronze y Silver. |

Todos los métodos de ejecución aceptan `name` para procesar un único dataset, lo que
resulta útil al depurar:

```python
engine.promote_silver("ventas_current")
```

## Contenido de la sección

| Página | Tema |
|---|---|
| [Carga batch](batch.md) | Contratos de Landing a Bronze y tipos de carga |
| [Carga streaming](streaming.md) | Structured Streaming, checkpoints y Kafka |
| [Promoción a Silver](silver.md) | Contratos de Bronze a Silver |
| [Estrategias](strategies.md) | Las cuatro formas de fusionar datos en Silver |
| [Columnas técnicas](metadata.md) | Columnas que añade el framework en Bronze y Silver |
