# Propiedades y máscaras

El objeto `properties` del contrato mezcla dos cosas: propiedades nativas de Delta y
opciones de comportamiento de DKOps. El loader las separa al cargar el contrato, de modo
que solo las propiedades reales de Delta llegan a `TBLPROPERTIES`.

| Clave | Tipo | Por defecto | Efecto |
|---|---|---|---|
| `delta.*` | string | | Se aplica como `TBLPROPERTIES` de Delta |
| `merge_schema` | bool | `false` | Activa `mergeSchema` en `append` y `overwrite_partition` |
| `change_data_feed` | bool | `false` | Activa `delta.enableChangeDataFeed` al crear la tabla |

## merge_schema

Permite que una escritura añada columnas nuevas a la tabla en lugar de fallar.

```json
{
  "properties": {
    "merge_schema": true
  }
}
```

```python
from pyspark.sql.functions import lit

df_nuevo = df.withColumn("canal_origen", lit(None).cast("STRING"))
TableWriter(contract).append(df_nuevo)   # la columna se añade al schema
```

Las filas anteriores tendrán la columna nueva a `null`.

!!! note "Solo en append y overwrite_partition"

    `overwrite` ya usa `overwriteSchema=true`, así que no necesita esta opción.

Recuerda declarar también la columna nueva en el contrato. Si no, la validación con
`strict_columns=True` la rechazará.

## change_data_feed

Activa el Change Data Feed de Delta, que guarda cada inserción, actualización y borrado
como una entrada consultable.

```json
{
  "properties": {
    "change_data_feed": true
  }
}
```

```python
cambios = TableReader(contract).read_cdf(starting_version=1)
cambios.select("producto_id", "_change_type", "_commit_version").show()
```

Más detalles en [Lectura](reader.md#change-data-feed).

## Máscaras de columna

Una columna con `mask` recibe, después de la escritura, la sentencia
`ALTER TABLE ... ALTER COLUMN ... SET MASK`.

```json
{
  "name": "email",
  "type": "STRING",
  "mask": "security.mask_email"
}
```

- La función debe existir antes en Unity Catalog. `security.mask_email` se resuelve como
  `<catalogo>.security.mask_email`.
- Solo tiene efecto en Databricks. En local y con `dry_run=True` se omite sin error.
- Se aplica al crear la tabla con `overwrite()` y cada vez que se llama a
  `apply_contract_metadata()`.

## Valores por defecto

Una columna con `default` añade una cláusula `DEFAULT` al `CREATE TABLE`. Es una forma
cómoda de registrar datos de auditoría:

```json
{ "name": "cargado_en",       "type": "TIMESTAMP", "default": "current_timestamp()" },
{ "name": "cargado_por",      "type": "STRING",    "default": "current_user()" },
{ "name": "version_pipeline", "type": "STRING",    "default": "'1.0.0'" }
```

Observa que los literales de texto van entre comillas simples dentro del JSON.

## Propiedades de Delta habituales

```json
{
  "properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "true"
  }
}
```

Los valores de propiedades Delta se escriben como texto, igual que en SQL.
