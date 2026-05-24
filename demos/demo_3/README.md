# Demo 3 — E-commerce

Pipeline Lakehouse completo para dominio de **e-commerce**. Demuestra las tres estrategias principales de promoción Silver, schema evolution automática con `merge_schema`, enmascaramiento de columnas sensibles y ingesta streaming.

```bash
python demos/demo_3/pipeline.py
```

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| `IngestionEngine` batch + streaming | `pipeline.py` — fases 2 y 2b |
| `full_merge` — catálogo de clientes | `clientes_current` |
| `cdc_merge` — pedidos CDC I/U/D | `pedidos_current` + `is_deleted` |
| `append_dedup` — clickstream | `eventos_current` — anti-join |
| `merge_schema: true` — schema evolution | `pedidos_raw` — columnas v2 sin recrear tabla |
| `mask` — column masking | `email_cliente` en clientes y pedidos |
| Streaming con `availableNow` | `eventos_web` vía `run_streaming()` |
| Auto-schema inference en streaming | `FileStreamReader` infiere desde archivos existentes |

---

## Estructura

```
demo_3/
├── pipeline.py                # orquestador — 6 fases
├── config/
│   └── config.json
├── datagen/
│   ├── main.py
│   ├── generate_clientes.py   # 300 clientes (50% activos)
│   ├── generate_pedidos.py    # 400 pedidos CDC (I/U/D)
│   └── generate_eventos.py    # 200 eventos web
├── ingestion/
│   ├── batch/                 # clientes.json, pedidos.json
│   ├── streaming/             # eventos_web.json
│   └── silver/                # clientes_current.json, pedidos_current.json, eventos_current.json
└── tables/
    ├── bronze/                # contratos Bronze
    ├── silver/                # contratos Silver
    └── gold/                  # ventas_canal.json, clientes_activos.json
```

---

## Flujo Landing → Bronze → Silver → Gold

```
Landing                 Bronze                  Silver                  Gold
─────────────────────────────────────────────────────────────────────────────────
clientes/ (300)    →    clientes_raw        →   clientes_current    →
                        (full snapshot)         (full_merge)            ventas_canal
pedidos/ (400)     →    pedidos_raw         →   pedidos_current     →   (3 filas)
                        (CDC, merge_schema)      (cdc_merge)
                                                                     →  clientes_activos
eventos_web/ (200) →    eventos_raw         →   eventos_current         (32 filas)
[STREAMING]             (streaming→batch)        (append_dedup)
```

---

## merge_schema — Schema evolution

El contrato `pedidos_raw.json` declara `"merge_schema": true`. Los pedidos se generan en dos versiones:

- **v1** (primera carga): 8 columnas base
- **v2** (segunda carga): 3 columnas nuevas — `metodo_envio`, `dias_entrega`, `calificacion`

```json
{
  "properties": { "merge_schema": true }
}
```

Sin `merge_schema`, la segunda carga lanzaría `AnalysisException`. Con él, Delta añade las columnas automáticamente y los registros anteriores las tienen como `null`.

---

## Column masking

```json
{
  "name": "email_cliente",
  "type": "STRING",
  "mask": "security.mask_email"
}
```

En Databricks / Unity Catalog, tras cada escritura el framework ejecuta:

```sql
ALTER TABLE ecommerce.clientes_current
  ALTER COLUMN email_cliente SET MASK security.mask_email;
```

En local PC la operación se omite silenciosamente — el pipeline corre sin cambios.

---

## Cómo ejecutarlo

```bash
# Desde la raíz del repositorio
python demos/demo_3/pipeline.py
```

Fases:
1. Genera datos en Landing (clientes JSON + pedidos CDC + eventos streaming)
2. Landing → Bronze batch (clientes + pedidos)
3. Landing → Bronze streaming (eventos, `availableNow`)
4. Bronze → Silver (3 estrategias)
5. Silver → Gold (ventas_canal, clientes_activos)
6. Validación y status

---

## Próximo paso

Demo 4 introduce `TableReader.read_cdf()` para detectar cambios via Change Data Feed y `SafeMigrator` para planificar migraciones de schema. Ver [demo_4/README.md](../demo_4/README.md).
