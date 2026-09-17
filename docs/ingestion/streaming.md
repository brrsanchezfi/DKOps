# Carga streaming

Los contratos streaming viven en `ingestion/streaming/`. Usan Structured Streaming y
guardan checkpoints, de modo que cada ejecución procesa solo lo que llegó desde la
anterior.

## Ejemplo

```json title="ingestion/streaming/eventos.json"
{
  "name":        "eventos_app",
  "ingest_type": "streaming",
  "load_type":   "streaming",
  "trigger":     "available_now",
  "source": {
    "format": "json",
    "path":   "{path.landing}/eventos_app"
  },
  "destination_contract": "../../tables/bronze/eventos_raw.json"
}
```

## Modos de ejecución

| Modo | Cómo se lanza | Comportamiento |
|---|---|---|
| `available_now` | `engine.run_streaming()` | Procesa todo lo pendiente, se detiene y devuelve el control. Es el modo por defecto. |
| `continuous` | `engine.start_streaming()` | Deja las queries corriendo en segundo plano hasta que llames a `stop_streaming()`. |

```python
queries = engine.start_streaming()
# ... otras tareas batch ...
engine.stop_streaming(queries)
```

`available_now` encaja bien en jobs programados: se comporta como un batch, pero con la
garantía de los checkpoints de no reprocesar archivos.

## Checkpoints

Por defecto el checkpoint se guarda en `{path.checkpoint}/streaming/<name>`. Puedes
cambiarlo con `checkpoint_suffix`.

!!! warning "Cambiar el checkpoint reprocesa todo"

    Si modificas `checkpoint_suffix` o borras la carpeta, el stream vuelve a empezar
    desde el principio y leerá de nuevo todos los archivos de Landing.

## Schema de la fuente

`readStream` no admite `inferSchema`. Si el contrato no declara `source.schema`,
`FileStreamReader` lee los archivos ya presentes con una lectura estática, toma su schema
y lo aplica al stream. Esto funciona siempre que haya al menos un archivo en la ruta.

Para evitar sorpresas, en especial en local, declara el schema de forma explícita:

```json
"source": {
  "format": "json",
  "path":   "{path.landing}/eventos_app",
  "schema": [
    { "name": "evento_id",  "type": "string" },
    { "name": "cliente_id", "type": "string" },
    { "name": "ts",         "type": "timestamp" }
  ]
}
```

En Databricks el reader es Auto Loader, que gestiona el schema por su cuenta en la ruta
indicada con `schema_root`.

## Kafka

Con `"format": "kafka"` se usa `KafkaReader` en cualquier entorno. La configuración del
tópico va en `source.kafka`:

```json
{
  "name":        "sensores",
  "ingest_type": "streaming",
  "source": {
    "format": "kafka",
    "kafka": {
      "topic":             "planta.sensores",
      "starting_offsets":  "earliest",
      "bootstrap_servers": "broker:9092"
    }
  },
  "metadata": { "add_kafka_metadata": true },
  "destination_contract": "../../tables/bronze/sensores_raw.json"
}
```

Las credenciales no deben ir en el contrato. Pásalas al construir el engine, idealmente
leyéndolas de Databricks Secrets o de variables de entorno:

```python
engine = IngestionEngine.from_launcher(
    streaming_contracts_dir = "ingestion/streaming",
    kafka_creds = {
        "bootstrap.servers": servidores,
        "sasl.username":     usuario,
        "sasl.password":     clave,
    },
)
```

En local necesitas el conector de Kafka en el classpath de Spark
(`org.apache.spark:spark-sql-kafka-0-10_2.12`). En Databricks ya viene incluido.

## Batch o streaming

| | Batch | Streaming |
|---|---|---|
| Lectura | `spark.read` | `readStream` |
| Qué procesa | Todos los archivos del directorio | Solo los nuevos desde el último checkpoint |
| Idempotencia | Sobrescritura de la partición del día | Checkpoint |
| Latencia típica | Minutos u horas | Segundos o minutos |
| Casos de uso | Cargas diarias, ETL nocturno | Clickstream, IoT, eventos |
