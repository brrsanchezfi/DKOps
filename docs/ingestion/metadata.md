# Columnas técnicas

Además de los datos de negocio, el framework añade columnas que permiten saber de dónde
salió cada fila y cuándo se escribió.

## En Bronze

Se controlan desde el bloque `metadata` del contrato de ingesta.

| Opción | Columna | Tipo | Por defecto |
|---|---|---|---|
| `add_ingested_at` | `_ingested_at` | `TIMESTAMP` | Activada |
| `add_ingested_date` | `_ingested_date` | `DATE` | Activada |
| `add_source_file` | `_source_file` | `STRING` | Activada |
| `add_kafka_metadata` | tópico, partición y offset | varios | Desactivada, solo con Kafka |

`_ingested_date` es además la columna de partición de Bronze y la que hace idempotente la
carga. No la desactives en contratos de Bronze.

Estas columnas se quedan en Bronze: las estrategias las descartan al escribir en Silver.

## En Silver

Con `"add_silver_timestamps": true` en el contrato de promoción, todas las estrategias
pueden añadir:

| Columna | Tipo | Significado |
|---|---|---|
| `_silver_created_at` | `TIMESTAMP` | Primera vez que la fila se escribió en Silver |
| `_silver_modified_at` | `TIMESTAMP` | Última vez que la fila cambió en Silver |

Solo se materializan las que el contrato de la tabla Silver declare. Si tu contrato lista
únicamente `_silver_modified_at`, la otra se descarta.

```json title="tables/silver/ventas_current.json (fragmento)"
{ "name": "_silver_created_at",  "type": "TIMESTAMP", "nullable": true,
  "comment": "Primera escritura en Silver" },
{ "name": "_silver_modified_at", "type": "TIMESTAMP", "nullable": true,
  "comment": "Última actualización en Silver" }
```

!!! warning "El significado de `_silver_created_at` depende de la estrategia"

    | Estrategia | Qué representa |
    |---|---|
    | `full_merge` | Primera inserción de la clave. Se conserva en los UPDATE. |
    | `cdc_merge` | Primera inserción de la clave. Se conserva en los UPDATE. |
    | `append_dedup` | Primera inserción. Esta estrategia solo inserta. |
    | `incremental_replace` | Última reconstrucción de la partición, no la primera vez que se vio la fila. |

    En `full_merge` y `cdc_merge` la columna se conserva porque se pasa como
    `insert_only_columns` al MERGE. Ver [Escritura](../governance/writer.md#columnas-que-solo-se-insertan).

## Borrado lógico

La estrategia `cdc_merge` gestiona además:

| Columna | Tipo | Significado |
|---|---|---|
| `is_deleted` | `BOOLEAN` | `true` cuando la fuente envió un evento `D` para esa clave |
