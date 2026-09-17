# Logger de aplicación

Todas las clases del framework heredan de `LoggableMixin` y escriben con `self.log`. Si
extiendes DKOps o escribes tus propias transformaciones, haz lo mismo en lugar de usar
`print()` o el módulo `logging`.

```python
from DKOps.logger_config import LoggableMixin

class MiTransformacion(LoggableMixin):
    def ejecutar(self, df):
        self.log.info(f"Procesando {df.count():,} filas")
        return df
```

`self.log` es un logger de Loguru asociado al nombre de la clase, así que cada línea ya
indica de dónde viene:

```
2026-09-01 10:14:22 | INFO | MiTransformacion.ejecutar | Procesando 12.400 filas
```

## Helpers semánticos

Además de `info`, `debug`, `warning` y `error`, el mixin incluye métodos que producen un
formato uniforme. Es preferible usarlos a componer el mensaje a mano.

| Helper | Cuándo usarlo |
|---|---|
| `log_start(op, **ctx)` | Al empezar una operación |
| `log_end(op, elapsed_s, **ctx)` | Al terminar, con la duración si la tienes |
| `log_read_ok(op, rows, source)` | Después de una lectura correcta |
| `log_write_ok(op, rows, target, mode)` | Después de una escritura correcta |
| `log_transform_ok(op, ...)` | Tras una transformación, con filas de entrada y salida |
| `log_skip(op, reason)` | Cuando se omite algo a propósito |
| `log_warning(op, message, **ctx)` | Para una advertencia dentro de una operación |
| `log_error(op, exc, **ctx)` | Para un error, con la traza completa |

```python
class LectorVentas(LoggableMixin):
    def leer(self, path):
        self.log_start("lectura", source=path)
        df = self._spark.read.parquet(path)
        self.log_read_ok("lectura", rows=df.count(), source=path)
        return df
```

## El decorador log_operation

Si lo único que necesitas es marcar el inicio, el fin, la duración y los errores de un
método, el decorador lo hace por ti:

```python
from DKOps.logger_config import LoggableMixin, log_operation

class Pipeline(LoggableMixin):
    @log_operation("normalización de fechas")
    def normalizar(self, df):
        return df.withColumn(...)

    @log_operation(log_args=True)      # incluye los argumentos en el log
    def run(self, table: str, date: str):
        ...
```

Genera una línea de inicio y otra de fin con el tiempo medido. Si el método lanza una
excepción, la registra con su traza. Funciona también en clases que no heredan del mixin.

## Configuración

No hay que inicializar nada: `Launcher` llama a `AppLogger.setup()` y añade el handler de
archivo. Los valores se leen de `config.json`:

```json
{
  "LOG_LEVEL":     "INFO",
  "LOG_DIR":       "/tmp/logs",
  "LOG_ROTATION":  "10 MB",
  "LOG_RETENTION": "7 days",
  "LOG_SERIALIZE": false
}
```

| Clave | Descripción |
|---|---|
| `LOG_LEVEL` | Nivel mínimo: `DEBUG`, `INFO`, `WARNING` o `ERROR` |
| `LOG_DIR` | Carpeta de los archivos de log. Puede ser una ruta cloud, ver [Logs en la nube](cloud-logs.md) |
| `LOG_ROTATION` | Tamaño o periodo a partir del cual se rota el archivo |
| `LOG_RETENTION` | Cuánto tiempo se conservan los archivos rotados |
| `LOG_SERIALIZE` | Con `true`, cada línea se emite como JSON. Útil para herramientas de observabilidad |

!!! tip "Usa DEBUG cuando una escritura no hace lo esperado"

    Los writers registran en nivel `DEBUG` el SQL que ejecutan: el `MERGE INTO` completo,
    el DDL y las sentencias de comentarios. Es lo primero que conviene revisar.
