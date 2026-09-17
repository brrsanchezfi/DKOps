# Contratos de tabla

<p class="dk-lead">
Un contrato de tabla es un JSON con el estado deseado de una tabla Delta: columnas,
particiones, propiedades y permisos. <code>load_contract()</code> lo convierte en un
<code>TableContract</code> inmutable que consumen los writers, los readers y el
<code>SafeMigrator</code>.
</p>

## Ejemplo completo

```json title="tables/bronze/fact_vuelos.json"
{
  "catalog":  "{catalog.bronze}",
  "schema":   "aeronautica",
  "name":     "fact_vuelos",
  "type":     "EXTERNAL",
  "format":   "DELTA",
  "comment":  "Hechos de vuelos operacionales",
  "owner":    "data-engineers",
  "location": "{path.bronze}/aeronautica/fact_vuelos",
  "columns": [
    { "name": "vuelo_id",   "type": "STRING",    "nullable": false },
    { "name": "fecha",      "type": "DATE",      "nullable": false },
    { "name": "origen",     "type": "STRING" },
    { "name": "email_pax",  "type": "STRING",    "mask": "security.mask_email" },
    { "name": "cargado_en", "type": "TIMESTAMP", "default": "current_timestamp()" }
  ],
  "partitions": ["fecha"],
  "properties": {
    "delta.autoOptimize.optimizeWrite": "true",
    "merge_schema":     true,
    "change_data_feed": true
  },
  "permissions": [
    { "action": "SELECT", "principal": "analysts-group", "operation": "GRANT" }
  ]
}
```

Para cargarlo:

```python
from DKOps.table_governance import load_contract

contract = load_contract("tables/bronze/fact_vuelos.json")
contract.effective_name   # nombre correcto para el runtime actual
```

## Campos de la tabla

| Campo | Tipo | Por defecto | Descripción |
|---|---|---|---|
| `catalog` | string | | Catálogo de destino. Admite placeholders. |
| `schema` | string | | Schema o base de datos. |
| `name` | string | | Nombre de la tabla. |
| `type` | string | `MANAGED` | `MANAGED` o `EXTERNAL`. |
| `format` | string | `DELTA` | Formato de almacenamiento. |
| `comment` | string | | Descripción de la tabla. |
| `owner` | string | | Propietario en Unity Catalog. |
| `location` | string | | Ruta física, necesaria en tablas `EXTERNAL`. |
| `columns` | array | | Lista de columnas, ver abajo. |
| `partitions` | array | `[]` | Columnas de partición. Deben existir en `columns`. |
| `clustering` | object | | Liquid clustering en Databricks, como alternativa al particionado: `{"columns": [...]}`. |
| `properties` | object | `{}` | Propiedades Delta y opciones de DKOps. Ver [Propiedades y máscaras](properties.md). |
| `permissions` | array | `[]` | Permisos de Unity Catalog. |

## Campos de columna

| Campo | Tipo | Por defecto | Descripción |
|---|---|---|---|
| `name` | string | | Nombre de la columna. |
| `type` | string | | Tipo de Spark. |
| `nullable` | bool | `true` | Si admite nulos. |
| `comment` | string | | Descripción de la columna. |
| `default` | string | | Expresión SQL usada como valor por defecto. |
| `mask` | string | | Función de máscara de Unity Catalog. |

Tipos admitidos: `STRING`, `INTEGER`, `LONG`, `DOUBLE`, `FLOAT`, `BOOLEAN`, `DATE`,
`TIMESTAMP`, `BINARY`, `DECIMAL`, `ARRAY`, `MAP` y `STRUCT`.

## Permisos

Cada entrada de `permissions` se traduce en una sentencia `GRANT` o `REVOKE`:

| Campo | Valores |
|---|---|
| `action` | `SELECT`, `MODIFY`, `CREATE`, `READ_METADATA`, `ALL PRIVILEGES`, `USAGE` o `EXECUTE` |
| `principal` | Un grupo o una service principal. Obligatorio. |
| `operation` | `GRANT` (por defecto) o `REVOKE` |

Los permisos solo se aplican en Databricks. En local se omiten sin error.

## Contenido de la sección

| Página | Tema |
|---|---|
| [Propiedades y máscaras](properties.md) | `merge_schema`, Change Data Feed, máscaras y valores por defecto |
| [Escritura](writer.md) | `TableWriter` y sus operaciones |
| [Metadata y tablas externas](metadata.md) | `apply_contract_metadata()` y tablas `EXTERNAL` |
| [Lectura](reader.md) | `TableReader` |
| [Migraciones](migrations.md) | `SafeMigrator` |
