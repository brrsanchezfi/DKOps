# Carga batch

Los contratos batch viven en `ingestion/batch/` y describen cómo leer una fuente de
Landing y en qué tabla de Bronze dejarla.

## Ejemplo

```json title="ingestion/batch/ventas.json"
{
  "name":        "ventas_diarias",
  "ingest_type": "batch",
  "load_type":   "incremental",
  "enabled":     true,
  "source": {
    "format": "json",
    "path":   "{path.landing}/ventas_diarias"
  },
  "destination_contract": "../../tables/bronze/ventas_raw.json",
  "metadata": {
    "add_ingested_at":   true,
    "add_ingested_date": true,
    "add_source_file":   true
  }
}
```

## Campos

| Campo | Obligatorio | Descripción |
|---|---|---|
| `name` | Sí | Identificador del dataset. Aparece en los logs y en la tabla de control. |
| `destination_contract` | Sí | Ruta al contrato de la tabla Bronze, relativa a este archivo. |
| `source.format` | No | `json` (por defecto), `csv`, `parquet`, `avro`, `delta` o `kafka`. |
| `source.path` | Sí, salvo Kafka | Ruta en Landing. Lo normal es usar `{path.landing}`. |
| `source.options` | No | Opciones que se pasan tal cual al reader de Spark, por ejemplo `{"header": "true"}` para CSV. |
| `source.schema` | No | Schema explícito como lista de `{"name", "type"}`. |
| `ingest_type` | No | `batch` (por defecto) o `streaming`. |
| `load_type` | No | `incremental` (por defecto), `full` o `cdc`. |
| `enabled` | No | Con `false` el contrato se omite sin error. |
| `filter` | No | Expresión SQL que se aplica al leer la fuente. |
| `metadata` | No | Columnas técnicas que se añaden. Ver [Columnas técnicas](metadata.md). |
| `description` | No | Texto libre. |

## Tipos de carga

| `load_type` | Cuándo usarlo | Comportamiento |
|---|---|---|
| `incremental` | Llegan archivos nuevos cada día | Lee el directorio y sobrescribe la partición `_ingested_date` del día |
| `full` | Cada entrega es un snapshot completo | Igual que `incremental`; la diferencia es semántica y sirve para documentar |
| `cdc` | La fuente trae eventos de cambio | Espera un campo `op_type` con valores `I`, `U` o `D` |

## Por qué es idempotente

Bronze está particionado por `_ingested_date`. Cada ejecución reemplaza únicamente la
partición del día en curso, así que lanzar la carga dos veces el mismo día deja la tabla
exactamente igual. Por esa razón no conviene desactivar `add_ingested_date` en contratos
de Bronze.

## Ejemplo con CSV

```json
{
  "name": "lotes_produccion",
  "source": {
    "format":  "csv",
    "path":    "{path.landing}/lotes_produccion",
    "options": { "header": "true", "inferSchema": "true" }
  },
  "destination_contract": "../../tables/bronze/lotes_produccion_raw.json"
}
```
