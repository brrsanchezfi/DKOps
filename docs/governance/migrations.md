# Migraciones

Con el tiempo los contratos cambian: aparece una columna, se corrige un comentario o se
añade un permiso. `SafeMigrator` compara el contrato con el estado real de la tabla y
genera solo los cambios que se pueden aplicar sin perder datos.

## Uso

```python
from DKOps.table_governance import load_contract, SafeMigrator

contract = load_contract("tables/silver/ventas_current.json")

# Ver el plan sin ejecutar nada
SafeMigrator(contract, dry_run=True).apply()

# Aplicar los cambios
SafeMigrator(contract).apply()
```

Si solo quieres el plan como objeto, sin imprimir ni ejecutar:

```python
plan = SafeMigrator(contract).plan()

if not plan.is_empty:
    for op in plan.operations:
        print(op.kind, op.sql)
```

## Qué puede cambiar

| Cambio en el contrato | ¿Lo aplica? | Sentencia |
|---|---|---|
| Columna nueva | Sí | `ALTER TABLE ... ADD COLUMN` |
| Comentario de columna | Sí | `ALTER COLUMN ... COMMENT` |
| Comentario de tabla | Sí | `SET TBLPROPERTIES ('comment' = ...)` |
| Propiedades Delta | Sí | `SET TBLPROPERTIES` |
| Permisos | Sí, solo en Databricks | `GRANT` o `REVOKE` |
| Eliminar una columna | No | |
| Cambiar el tipo de una columna | No | |
| Cambiar las particiones | No | |

Los cambios que no aparecen como seguros implican reescribir o perder datos, así que
quedan fuera a propósito. Si los necesitas, hazlos como una migración explícita.

## Ejemplo de salida

```
Plan de migración para 'silver.ventas_current' (2 operación(es)):
──────────────────────────────────────────────────────────────────────
  1. [ADD_COLUMN] Nueva columna: canal_origen STRING
     SQL: ALTER TABLE silver.ventas_current ADD COLUMN `canal_origen` STRING
  2. [CHANGE_COMMENT] Actualizar comentario de 'venta_id'
     SQL: ALTER TABLE silver.ventas_current ALTER COLUMN `venta_id` COMMENT 'Id de la venta'
──────────────────────────────────────────────────────────────────────
```

## Comportamiento a tener en cuenta

- Si la tabla no existe, el plan sale vacío y se registra un aviso. Para crearla usa
  `TableWriter.overwrite()`.
- Si una operación falla al aplicarse, se registra el error y se continúa con las
  siguientes. Revisa el log después de migrar.
- Un plan vacío significa que el contrato y la tabla están alineados.

Una buena práctica es ejecutar el plan en `dry_run` dentro de la integración continua y
revisarlo antes de aplicarlo en producción.
