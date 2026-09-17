# Logs en la nube

`LOG_DIR` puede apuntar a almacenamiento cloud (`abfss://`, `s3://` o `gs://`). DKOps
elige el mecanismo de escritura según lo que ofrezca el cluster.

## Escritura por tramos

En Databricks con Spark Connect no hay puente a la JVM, así que el log se escribe con
`dbutils.fs.put`. Cada sincronización crea un objeto nuevo en lugar de reescribir el
mismo archivo:

```
_logs/streaming/ingest_bronze.20260906T023042Z-a1b2c3.0001.log
                ingest_bronze.20260906T023042Z-a1b2c3.0002.log
                ingest_bronze.20260906T023042Z-a1b2c3.0003.log
```

El nombre incluye un identificador de la ejecución y un número de tramo con ceros a la
izquierda. Eso tiene dos ventajas: ordenar por nombre equivale a ordenar por tiempo, y
dos procesos que escriben el mismo log no se pisan.

Si la escritura de un tramo falla, vuelve a la cola y se reintenta en la siguiente
sincronización.

??? info "Por qué no se escribe un único archivo"

    Hasta la v0.3.4 cada sincronización reescribía el archivo completo con
    `overwrite=True`. Esa operación vacía el destino antes de escribir y devuelve el
    control antes de que el blob esté confirmado. Si el proceso moría en ese intervalo,
    el archivo quedaba con 0 bytes y se perdía todo el histórico, no solo el último
    fragmento.

    Escribir tramos nuevos no depende de que el almacenamiento ofrezca escritura o
    renombrado atómicos (en ADLS el renombrado solo es atómico con espacio de nombres
    jerárquico). En el peor caso se pierde el último tramo.

## Leer un log completo

```python
from DKOps.logger_config import AppLogger

texto = AppLogger.read_cloud_log(spark, log_dir, "ingest_bronze")
print(texto)
```

Sin más argumentos concatena todas las ejecuciones, separadas por una cabecera. Para una
ejecución concreta:

```python
AppLogger.read_cloud_log(spark, log_dir, "ingest_bronze", run="20260906T023042Z-a1b2c3")
```

## Forzar el volcado antes de terminar

El volcado final ocurre al cerrarse el proceso, pero en ese momento ya no hay nadie que
pueda reaccionar a un error de escritura. Si quieres asegurarte de que el log está
completo mientras el proceso sigue vivo, llama a `flush()`:

```python
from DKOps.logger_config import AppLogger

try:
    engine.ingest_bronze()
    engine.promote_silver()
finally:
    AppLogger.flush()
```

No hace nada si el handler activo no escribe por tramos o si no hay contenido pendiente.

Los fallos de escritura se informan por `stdout`, que es lo que Databricks captura y lo
que sigue disponible durante el apagado del intérprete.
