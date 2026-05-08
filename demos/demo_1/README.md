# Demo 1 — Contratos de tabla y writers gobernados

Demostración de referencia del framework **DKOps**: define cuatro tablas mediante contratos JSON y ejercita los cinco writers (`Create`, `Append`, `Upsert`, `Partition`, `Delete`) más el `SafeMigrator` sobre un dominio aeronáutico simulado.

Pensado como **referencia de uso** — no como tutorial paso a paso. Si ya conoces el framework y necesitas ver cómo se compone un pipeline completo, este demo es el patrón canónico.

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| Carga de contratos con placeholders (`{catalog.bronze}`, `{path.bronze}`) | `tables/*.json` + fase de carga |
| Resolución automática de runtime (local PC ↔ Databricks) | El mismo código corre en ambos sin cambios |
| Escritura full load (`CREATE OR REPLACE`) | Fase 1 — bootstrap de las 4 tablas |
| Escritura incremental (`APPEND`) | Fase 1 (días 2–7) y Fase 2 (día 8) |
| Upsert con MERGE (`UPSERT`) | Fase 2 — corrección de retrasos y estado de aerolínea |
| Reemplazo de partición específica | Fase 3 — reproceso del día 5 |
| Borrado por condición SQL (`DELETE`) | Fase 4 — limpieza de datos corruptos |
| Migración no destructiva de schema | Fase 5 — `SafeMigrator` en `dry_run` |
| Joins entre fact y dimensiones | Fase 6 — queries de validación |

---

## Estructura del demo

```
demo-1/
├── pipeline_aeronautica.py     # orquestador — ejecuta las 6 fases
├── data_generator.py            # genera datos sintéticos (15 aeropuertos, 8 aerolíneas, vuelos)
├── config/
│   └── config.json              # config del Launcher (env, paths, catálogos)
└── tables/
    ├── dim_aeropuertos.json     # contrato dimensión aeropuertos
    ├── dim_aerolineas.json      # contrato dimensión aerolíneas
    ├── dim_tiempo.json          # contrato dimensión tiempo
    └── fact_vuelos.json         # contrato hechos de vuelos (particionada por fecha)
```

---

## Modelo de datos

Esquema en estrella simple — una fact particionada por fecha y tres dimensiones.

```
            ┌──────────────────┐
            │  dim_aeropuertos │   15 aeropuertos (Colombia + LATAM + EEUU + EU)
            │  PK: iata_code   │
            └────────┬─────────┘
                     │
                     │ iata_origen / iata_destino
                     ▼
   ┌──────────────┐  fact_vuelos  ┌──────────────────┐
   │ dim_tiempo   │◄──────────────┤  fact_vuelos     │
   │ PK: fecha    │  fecha        │  particionada    │
   └──────────────┘               │  por fecha       │
                                  │  PK: vuelo_id    │
                                  └────────┬─────────┘
                                           │ iata_aerolinea
                                           ▼
                                  ┌──────────────────┐
                                  │  dim_aerolineas  │   8 aerolíneas
                                  │  PK: iata_code   │
                                  └──────────────────┘
```

Las tablas viven en el catálogo `bronze`, schema `aeronautica`. En local PC se mapean a `bronze.aeronautica.*` del catálogo nativo de Spark; en Databricks a Unity Catalog.

---

## Cómo correrlo

### Prerrequisitos

- Python 3.10+
- PySpark 3.5.x (recomendado — el `config.json` usa Delta 3.2.0)
- El paquete `DKOps` instalado o en el `PYTHONPATH`

### Local PC

```bash
cd demo-1
python3 pipeline_aeronautica.py
```

El primer arranque baja los JARs de Delta (~30 s la primera vez). Las siguientes ejecuciones son inmediatas porque el cache de Ivy queda en `~/.ivy2/cache`.

### Databricks Connect

Edita `config/config.json` con tus credenciales:

```json
{
  "EXECUTION_ENVIRONMENT": "databricks",
  "CLUSTER_ID": "<tu-cluster-id>",
  "DATABRICKS_HOST": "https://<workspace>.azuredatabricks.net",
  "DATABRICKS_TOKEN": "<tu-pat>"
}
```

El mismo `pipeline_aeronautica.py` corre sin cambios — el framework detecta el runtime y resuelve `effective_name` automáticamente.

---

## Las 6 fases del pipeline

### Fase 1 — Bootstrap

Crea las cuatro tablas desde cero. Las dimensiones se cargan completas (`CreateWriter`). Para `fact_vuelos` se hace `CREATE` con el primer día (2024-01-01) y luego seis `APPEND` consecutivos hasta el 2024-01-07. Resultado: ~560 vuelos distribuidos en una semana.

### Fase 2 — Operación diaria

Simula el flujo continuo de un pipeline en producción:

1. `APPEND` — vuelos nuevos del día 8.
2. `UPSERT` con `merge_keys=["vuelo_id", "fecha"]` — correcciones a 15 vuelos del día 3. Solo se actualizan las columnas de retraso y estado, conservando el resto. Se imprime un *antes/después* para verificar el efecto.
3. `UPSERT` sobre `dim_aerolineas` — Viva Air pasa a `activa=false`. Demuestra el caso 1-fila tipo SCD1 con `update_columns=["activa"]`.

### Fase 3 — Reprocesamiento de partición

`PartitionWriter` reemplaza solo la partición `fecha = 2024-01-05` con datos regenerados. Activa internamente `spark.sql.sources.partitionOverwriteMode=dynamic` y lo restaura al terminar. El resto de particiones queda intacto.

### Fase 4 — Limpieza

Inserta dos vuelos con `distancia_km = 0` (datos corruptos) vía `AppendWriter`. Después `DeleteWriter` los elimina con la condición SQL `distancia_km = 0 OR distancia_km IS NULL`. Reporta el número de filas afectadas leído del Delta log.

### Fase 5 — Evolución de schema

`SafeMigrator(contract, dry_run=True).apply()` para `fact_vuelos` y `dim_aeropuertos`. Genera un plan que detectaría columnas nuevas, comentarios cambiados, propiedades de tabla y permisos. En esta ejecución el plan sale vacío porque el contrato y la tabla están alineados — para verlo en acción, modifica un comentario en el JSON y vuelve a correr.

### Fase 6 — Validación

Cinco queries de negocio cruzando fact con las tres dimensiones:

- Top 5 rutas con mayor retraso promedio.
- Ocupación por aerolínea (con alianza desde `dim_aerolineas`).
- Vuelos por día con día de la semana y bandera de festivo (con `dim_tiempo`).
- Aeropuertos más transitados (operaciones origen + destino).
- Resumen ejecutivo del lakehouse.

---

## Patrones de uso del framework

### 1. Inicialización mínima

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, CreateWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_vuelos.json")

CreateWriter(contract).write(df)
```

El `Launcher` se auto-registra como singleton del proceso. Los writers, loaders y migrator obtienen `spark` y `env` internamente vía `Launcher.current()` — no hay que pasarlos.

### 2. Nombre efectivo de la tabla en SQL

```python
spark.sql(f"SELECT * FROM {contract.effective_name} WHERE fecha = '2024-01-15'")
```

`effective_name` resuelve `catalog.schema.name` en Databricks y `schema.name` en local PC. Úsalo siempre en SQL directo — nunca construyas el nombre a mano.

### 3. Contratos con placeholders

```json
{
  "catalog":  "{catalog.bronze}",
  "location": "{path.bronze}/aeronautica/fact_vuelos"
}
```

`{catalog.bronze}` y `{path.bronze}` se resuelven contra `config.json` según el workspace activo. El mismo contrato sirve para `dev` y `prod` — solo cambia el environment.

### 4. Columnas con default

```json
{
  "name":    "cargado_en",
  "type":    "TIMESTAMP",
  "default": "current_timestamp()"
}
```

Si la columna falta en el DataFrame, el writer la añade automáticamente con la expresión SQL del contrato. Útil para timestamps de ingesta, banderas de origen, etc.

### 5. Dry-run para producción

Todos los writers y el migrator aceptan `dry_run=True`:

```python
CreateWriter(contract, dry_run=True).write(df)
SafeMigrator(contract, dry_run=True).apply()
```

Ejecutan validación, planificación y logging completo — pero no escriben nada. Útil para CI o para revisar planes antes de aplicar.

---

## Salida esperada

Tras una ejecución exitosa verás logs estructurados de cada fase (vía `loguru`) terminando en:

```
┌──────────────────────────────────────────────┐
│           RESUMEN DEL LAKEHOUSE              │
├──────────────────────────────────────────────┤
│  Aeropuertos en dim     :     15             │
│  Aerolíneas en dim      :      8             │
│  Días en calendario     :      7             │
│  Total vuelos (fact)    :   ~640             │
│  Días con vuelos        :      8             │
│  Retraso prom. global   :  ~12.5 min         │
│  Ocupación prom. global :  ~78.3 %           │
└──────────────────────────────────────────────┘

✔ Pipeline completado exitosamente
```

Los números varían entre ejecuciones porque `data_generator.py` usa aleatoriedad sin seed fijo — es intencional, simula un entorno productivo donde cada corrida produce datos nuevos.

---

## Idempotencia

El demo es **completamente idempotente**: puedes correrlo cuantas veces quieras sin limpiar nada manualmente. La Fase 1 hace `CREATE OR REPLACE` y `DROP` previo de los paths Delta locales, dejando el lakehouse en estado limpio en cada arranque.

---
