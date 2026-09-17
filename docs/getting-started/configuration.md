# Configuración

Cada proyecto tiene un `config.json` con la parte de infraestructura: en qué entorno
corre, qué catálogos y rutas usa y cómo se registran los logs. Normalmente vive en
`config/config.json` y queda fuera de git.

## Ejemplo completo

```json
{
  "EXECUTION_ENVIRONMENT": "local",
  "SPARK_APP_NAME":        "MiPipeline",
  "SPARK_WAREHOUSE_DIR":   "/tmp/mi-pipeline/warehouse",
  "DELTA_VERSION":         "3.2.0",

  "LOG_LEVEL":     "INFO",
  "LOG_DIR":       "/tmp/logs",
  "LOG_ROTATION":  "10 MB",
  "LOG_RETENTION": "7 days",

  "environments": {
    "local": {
      "env":       "local",
      "env_short": "l",
      "catalogs": {
        "bronze": "bronze",
        "silver": "silver",
        "gold":   "gold"
      },
      "paths": {
        "landing":    "/tmp/mi-pipeline/landing",
        "bronze":     "/tmp/mi-pipeline/bronze",
        "silver":     "/tmp/mi-pipeline/silver",
        "gold":       "/tmp/mi-pipeline/gold",
        "checkpoint": "/tmp/mi-pipeline/checkpoints",
        "ops":        "/tmp/mi-pipeline/ops"
      }
    }
  }
}
```

## Claves principales

| Clave | Para qué sirve |
|---|---|
| `EXECUTION_ENVIRONMENT` | Indica si el proceso corre en local o en Databricks |
| `SPARK_APP_NAME` | Nombre de la aplicación Spark |
| `SPARK_WAREHOUSE_DIR` | Warehouse de Spark cuando se trabaja en local |
| `DELTA_VERSION` | Versión de los JAR de Delta que se descargan en local |
| `LOG_*` | Nivel, carpeta, rotación y retención del log de aplicación |
| `environments` | Un bloque por entorno con sus catálogos y rutas |

La configuración del logging se explica con detalle en
[Logger de aplicación](../operations/app-logger.md).

## Placeholders

Los valores del bloque `environments` se inyectan en los contratos JSON al cargarlos.
Así un mismo contrato sirve para desarrollo y producción.

| Placeholder | Se resuelve con |
|---|---|
| `{catalog.<capa>}` | `environments.<target>.catalogs.<capa>` |
| `{path.<nombre>}` | `environments.<target>.paths.<nombre>` |
| `{env}` | `environments.<target>.env` |
| `{env_short}` | `environments.<target>.env_short` |

Por ejemplo, `"path": "{path.landing}/ventas"` se convierte en
`/tmp/mi-pipeline/landing/ventas` con la configuración anterior.

!!! warning "Un placeholder sin definir detiene la carga"

    Si un contrato usa `{path.raw}` y el entorno activo no declara `paths.raw`, el
    loader lanza `KeyError`. Es intencional: prefiere fallar al cargar el contrato
    antes que escribir en una ruta equivocada.

## Local frente a Databricks

En local los catálogos se ignoran y las tablas se crean como `schema.nombre` en el
warehouse de Spark. En Databricks con Unity Catalog se crean como
`catalogo.schema.nombre`, usando el catálogo que resuelva el placeholder. Encontrarás
más detalle en [Local y Databricks](../concepts/runtime.md).
