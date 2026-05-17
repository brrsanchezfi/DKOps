# Demo 3 — Schema evolution y enmascaramiento de columnas

Dominio **e-commerce**. Demuestra dos características de gobernanza avanzada de DKOps:

## Qué aprenderás

### merge_schema — Schema evolution automática

El contrato `pedidos_v1.json` / `pedidos_v2.json` tiene `"merge_schema": true`.

- **Fase 1**: se crea la tabla `pedidos` con el schema inicial (6 columnas).
- **Fase 2**: se hace `append` con un DataFrame que trae 3 columnas nuevas (`metodo_envio`, `dias_entrega`, `calificacion`). Sin `merge_schema` esto lanzaría un `AnalysisException`. Con `merge_schema=true`, Delta añade las columnas automáticamente y los registros anteriores las tienen como `null`.

```json
// pedidos_v1.json
{
  "merge_schema": true,
  "columns": [
    {"name": "pedido_id",    ...},
    {"name": "email_cliente","type": "STRING", "mask": "security.mask_email"},
    ...
  ]
}
```

```python
TableWriter(ct_pedidos_v1).overwrite(df_v1)   # crea tabla con 6 columnas
TableWriter(ct_pedidos_v2).append(df_v2)      # añade 3 columnas nuevas — sin error
```

### mask — Enmascaramiento de columnas

Los contratos `clientes.json` y `pedidos_v1.json` declaran `"mask"` en columnas sensibles.

```json
{"name": "email",    "type": "STRING", "mask": "security.mask_email"},
{"name": "telefono", "type": "STRING", "mask": "security.mask_phone"}
```

En Databricks / Unity Catalog, tras la escritura el framework ejecuta automáticamente:

```sql
ALTER TABLE ecommerce.clientes ALTER COLUMN email    SET MASK security.mask_email;
ALTER TABLE ecommerce.clientes ALTER COLUMN telefono SET MASK security.mask_phone;
```

En PC local la operación se omite silenciosamente — el pipeline corre sin cambios.

## Estructura

```
demo_3/
├── config/
│   └── config.json          # configuración local
├── tables/
│   ├── clientes.json        # dim clientes — mask en email y telefono
│   ├── pedidos_v1.json      # pedidos schema inicial — merge_schema + mask en email
│   └── pedidos_v2.json      # pedidos schema evolucionado — 3 columnas nuevas
├── data_generator.py        # genera DataFrames sintéticos de clientes y pedidos
├── pipeline_ecommerce.py    # pipeline principal (4 fases)
└── README.md
```

## Ejecutar

```bash
# Activar el venv local
source ../../.venv-local/bin/activate   # Linux / Mac / WSL
# ../../.venv-local/Scripts/activate    # Windows PowerShell

cd demos/demo_3
python pipeline_ecommerce.py
```

## Salida esperada

```
══════════════════════════════════════════════════════════════════════════
  FASE 2 — Schema evolution con merge_schema: true
══════════════════════════════════════════════════════════════════════════

  ── Generando 200 pedidos — schema v2 ──────────────────────────────────
    Schema v2 de pedidos: ['pedido_id', 'cliente_id', 'email_cliente',
                           'fecha_pedido', 'total_usd', 'estado',
                           'metodo_envio', 'dias_entrega', 'calificacion']

    Columnas nuevas que no estaban en v1: ['calificacion', 'dias_entrega', 'metodo_envio']

    Sin merge_schema esto lanzaría AnalysisException.
    Con merge_schema=True, Delta añade las columnas automáticamente.

    ✔ Append completado — 200 filas añadidas
    ✔ Total en tabla: 500 filas
    ✔ Schema final  : ['pedido_id', ..., 'metodo_envio', 'dias_entrega', 'calificacion', 'cargado_en']
```
