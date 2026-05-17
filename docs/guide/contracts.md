# Contratos de tabla

Un contrato de tabla es un archivo JSON que define el estado deseado de una tabla Delta: su schema, particionado, propiedades, permisos y reglas de gobernanza. El `ContractLoader` lo parsea en un `TableContract` inmutable que los writers y el `SafeMigrator` consumen.

## Estructura completa

```json
{
  "catalog":  "{catalog.bronze}",
  "schema":   "aeronautica",
  "name":     "fact_vuelos",
  "type":     "EXTERNAL",
  "format":   "DELTA",
  "comment":  "Hechos de vuelos operacionales",
  "owner":    "data-engineers",
  "location": "{path.raw}/aeronautica/fact_vuelos",
  "merge_schema": false,
  "columns": [
    {"name": "vuelo_id",   "type": "STRING",    "nullable": false},
    {"name": "fecha",      "type": "DATE",      "nullable": false},
    {"name": "origen",     "type": "STRING",    "nullable": true},
    {"name": "email_pax",  "type": "STRING",    "nullable": true, "mask": "security.mask_email"},
    {"name": "cargado_en", "type": "TIMESTAMP", "default": "current_timestamp()"}
  ],
  "partitions": ["fecha"],
  "properties": {"delta.autoOptimize.optimizeWrite": "true"},
  "permissions": [
    {"action": "SELECT", "principal": "analysts-group", "operation": "GRANT"}
  ]
}
```

## Campos del contrato

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `catalog` | string | — | Catálogo destino (soporta placeholder) |
| `schema` | string | — | Schema / base de datos |
| `name` | string | — | Nombre de la tabla |
| `type` | string | `MANAGED` | `MANAGED` o `EXTERNAL` |
| `format` | string | `DELTA` | Formato de almacenamiento |
| `comment` | string | `null` | Descripción de la tabla |
| `owner` | string | `null` | Propietario en Unity Catalog |
| `location` | string | `null` | Ruta para tablas `EXTERNAL` |
| `merge_schema` | bool | `false` | Activa `mergeSchema=true` en append/partition |
| `columns` | array | — | Definición de columnas (ver abajo) |
| `partitions` | array | `[]` | Columnas de partición |
| `properties` | object | `{}` | `TBLPROPERTIES` de Delta |
| `permissions` | array | `[]` | Permisos Unity Catalog |

## Campos de columna

| Campo | Tipo | Default | Descripción |
|---|---|---|---|
| `name` | string | — | Nombre de la columna |
| `type` | string | — | Tipo Spark (ver lista abajo) |
| `nullable` | bool | `true` | Si acepta nulos |
| `comment` | string | `null` | Descripción de la columna |
| `default` | string | `null` | Expresión SQL como valor por defecto |
| `mask` | string | `null` | Función de máscara Unity Catalog (solo Databricks) |

## merge_schema — Evolución de schema

Cuando `"merge_schema": true`, los writes de tipo `append` y `overwrite_partition` activan la opción `mergeSchema=true` de Delta Lake. Esto permite añadir columnas nuevas en el DataFrame sin que la escritura falle.

```json
{
  "merge_schema": true,
  "columns": [
    {"name": "id",    "type": "STRING"},
    {"name": "valor", "type": "DOUBLE"}
  ]
}
```

```python
df_evolucionado = df.withColumn("nueva_col", lit(None).cast("STRING"))
TableWriter(contract).append(df_evolucionado)   # OK — Delta añade la columna al schema
```

## Enmascaramiento de columnas (mask)

El campo `"mask"` en una columna aplica `ALTER TABLE … ALTER COLUMN … SET MASK` tras la escritura. La función de máscara debe ser una función de Unity Catalog con la forma `<schema>.<nombre>`.

```json
{
  "name":  "email",
  "type":  "STRING",
  "mask":  "security.mask_email"
}
```

- Solo se aplica en Databricks / Unity Catalog.
- Se omite silenciosamente en PC local y con `dry_run=True`.
- Se aplica automáticamente al usar `TableWriter.overwrite()`.

## Placeholders

| Placeholder | Resuelve a |
|---|---|
| `{catalog.bronze}` | catálogo bronze del ambiente activo |
| `{catalog.silver}` | catálogo silver del ambiente activo |
| `{catalog.gold}` | catálogo gold del ambiente activo |
| `{path.raw}` | path ADLS contenedor raw |
| `{env}` | nombre del ambiente (`dev`, `prod`) |

## Tipos soportados

`STRING` · `INTEGER` · `LONG` · `DOUBLE` · `FLOAT` · `BOOLEAN` · `DATE` · `TIMESTAMP` · `BINARY` · `DECIMAL` · `ARRAY` · `MAP` · `STRUCT`

## Columnas con default

Las columnas con `"default"` añaden una cláusula `DEFAULT` al `CREATE TABLE`. También se usan para inyectar metadatos de auditoría:

```json
{"name": "cargado_en",     "type": "TIMESTAMP", "default": "current_timestamp()"},
{"name": "cargado_por",    "type": "STRING",     "default": "current_user()"},
{"name": "pipeline_version","type": "STRING",    "default": "'1.0.0'"}
```
