# Demo 1 — Aeronáutica

Demostración de referencia del framework **DKOps**: define cuatro tablas mediante contratos JSON y ejercita los cinco writers (`overwrite`, `append`, `upsert`, `overwrite_partition`, `delete`) más el `SafeMigrator` sobre un dominio aeronáutico simulado.

Pensado como **referencia de uso** — no como tutorial paso a paso. Si ya conoces el framework y necesitas ver cómo se compone un pipeline completo con writers gobernados, este demo es el patrón canónico.

---

## ¿Qué demuestra?

| Concepto | Dónde se ve |
|---|---|
| Contratos con placeholders (`{catalog.bronze}`) | `tables/*.json` |
| Runtime-agnóstico (local ↔ Databricks) | El mismo `pipeline.py` sin cambios |
| `overwrite` — CREATE OR REPLACE | Fase 1 — bootstrap de las 4 tablas |
| `append` — INSERT INTO | Fases 1 (días 2–7) y 2 (día 8) |
| `upsert` — MERGE INTO | Fase 2 — corrección de retrasos y estado aerolínea |
| `overwrite_partition` — reemplazo de partición | Fase 3 — reproceso del día 5 |
| `delete` — DELETE WHERE | Fase 4 — limpieza de vuelos con `distancia_km = 0` |
| `SafeMigrator` dry_run | Fase 5 — plan de migración sin ejecutar |
| `effective_name` en SQL | `FROM {contract.effective_name}` — sin hardcoding |

---

## Estructura

```
demo_1/
├── pipeline.py              # orquestador — 6 fases
├── config/
│   └── config.json          # env, paths, catálogos
├── datagen/
│   ├── main.py
│   ├── generate_aeropuertos.py   # 15 aeropuertos
│   └── generate_vuelos.py        # ~640 vuelos (días 1–8)
└── tables/
    ├── dim_aeropuertos.json
    ├── dim_aerolineas.json
    ├── dim_tiempo.json
    └── fact_vuelos.json          # particionada por fecha
```

---

## Modelo de datos

Esquema en estrella — una fact particionada por fecha y tres dimensiones.

```
            ┌──────────────────┐
            │  dim_aeropuertos │   15 aeropuertos (Colombia + LATAM + EEUU + EU)
            │  PK: iata_code   │
            └────────┬─────────┘
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

Las tablas viven en el catálogo `bronze`, schema `aeronautica`. En local → `bronze.aeronautica.*`; en Databricks → Unity Catalog.

---

## Cómo ejecutarlo

```bash
# Desde la raíz del repositorio
python demos/demo_1/pipeline.py
```

El primer arranque descarga los JARs de Delta (~30 s). Las siguientes ejecuciones son inmediatas.

**El demo es completamente idempotente** — puedes correrlo cuantas veces quieras sin limpiar nada. La Fase 1 hace `CREATE OR REPLACE` dejando el lakehouse en estado limpio.

---

## Las 6 fases

| Fase | Operación | Writer |
|---|---|---|
| 1 — Bootstrap | Crea las 4 tablas + días 1–7 de vuelos | `overwrite` + `append` |
| 2 — Día 8 | Vuelos nuevos + corrección + Viva Air inactiva | `append` + `upsert` |
| 3 — Reproceso | Regenera solo `fecha = día 5` | `overwrite_partition` |
| 4 — Limpieza | Inserta y elimina vuelos corruptos | `append` + `delete` |
| 5 — Schema | Compara contrato vs tabla real | `SafeMigrator(dry_run=True)` |
| 6 — Validación | 5 queries de negocio cruzando fact y dims | SQL |

---

## Salida esperada

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

Los números varían entre ejecuciones porque el datagen usa aleatoriedad sin seed fijo.

---

## Patrones clave del framework

### Inicialización mínima

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter

launcher = Launcher("config/config.json")
contract = load_contract("tables/fact_vuelos.json")

TableWriter(contract).overwrite(df)
```

El `Launcher` se auto-registra como singleton. Los writers obtienen `spark` y `env` internamente vía `Launcher.current()`.

### `effective_name` en SQL

```python
spark.sql(f"SELECT * FROM {contract.effective_name} WHERE fecha = '2024-01-15'")
```

`effective_name` resuelve `catalog.schema.name` en Databricks y `schema.name` en local. Nunca construyas el nombre a mano.

### Dry-run

```python
SafeMigrator(contract, dry_run=True).apply()
TableWriter(contract, dry_run=True).overwrite(df)
```

Ejecutan validación y logging completo sin escribir nada. Útil para CI y revisión de planes.
