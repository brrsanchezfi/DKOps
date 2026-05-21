"""
pipeline_aeronautica.py
=======================
Pipeline completo del dominio aeronáutico.

Ejercita TODOS los métodos de TableWriter y el migrador en un flujo realista:

  FASE 1 — Bootstrap (primera ejecución)
    · writer.overwrite()           → dim_aeropuertos, dim_aerolineas, dim_tiempo
    · writer.overwrite()           → fact_vuelos (primer día)
    · writer.append()              → fact_vuelos (días restantes de la semana)
      fact_vuelos tiene merge_schema: true → acepta columnas nuevas en el DF

  FASE 2 — Operación diaria (simula día nuevo)
    · writer.append()              → fact_vuelos (vuelos del día nuevo)
    · writer.upsert(keys=[...])   → fact_vuelos (correcciones)
    · writer.upsert(keys=[...])   → dim_aerolineas (actualizar estado)

  FASE 3 — Reprocesamiento
    · writer.overwrite_partition() → fact_vuelos (reemplazar partición)

  FASE 4 — Limpieza
    · writer.delete()              → fact_vuelos (eliminar vuelos corruptos)

  FASE 5 — Evolución de schema
    · SafeMigrator                 → fact_vuelos (plan de migración en dry_run)

  FASE 6 — Validación final
    · Queries de negocio para verificar integridad del lakehouse

Ejecutar:
    python3 pipeline_aeronautica.py

Cada ejecución genera datos distintos (retrasos, pasajeros aleatorios).
"""

from datetime import date, timedelta

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

from DKOps.launcher import Launcher
from DKOps.table_governance import (
    load_contract,
    SchemaValidator,
    TableWriter,
    SafeMigrator,
)
from data_generator import DataGenerator


# ─────────────────────────────────────────────────────────────────────────────
# Configuración del pipeline
# ─────────────────────────────────────────────────────────────────────────────

FECHA_INICIO   = "2024-01-01"
FECHA_FIN_INIT = "2024-01-07"
FECHA_DIA_8    = "2024-01-08"
FECHA_REPROC   = "2024-01-05"


def _sep(titulo: str) -> None:
    print(f"\n{'═' * 65}")
    print(f"  {titulo}")
    print('═' * 65)


def _sub(titulo: str) -> None:
    print(f"\n  ── {titulo} {'─' * max(1, 55 - len(titulo))}")


# ─────────────────────────────────────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────────────────────────────────────

launcher = Launcher("config/config.json", log_filename="vuelosDiarios")
spark    = launcher.spark
gen      = DataGenerator(spark)


# ── Cargar todos los contratos ────────────────────────────────────────────────
_sep("Cargando contratos de tabla")

ct_aeropuertos = load_contract("tables/dim_aeropuertos.json")
ct_aerolineas  = load_contract("tables/dim_aerolineas.json")
ct_tiempo      = load_contract("tables/dim_tiempo.json")
ct_fact        = load_contract("tables/fact_vuelos.json")

for ct in [ct_aeropuertos, ct_aerolineas, ct_tiempo, ct_fact]:
    print(f"  ✔ {ct.effective_name:55s} cols={len(ct.columns)}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 — Bootstrap
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 1 — Bootstrap (carga inicial)")

# ── dim_aeropuertos ───────────────────────────────────────────────────────────
_sub("dim_aeropuertos → CREATE OR REPLACE (15 aeropuertos)")
df_aeropuertos = gen.aeropuertos()

result = SchemaValidator(ct_aeropuertos).validate(df_aeropuertos)
print(f"    Validación: {result.summary()}")

TableWriter(ct_aeropuertos).overwrite(df_aeropuertos)

spark.sql(
    f"SELECT iata_code, ciudad, pais "
    f"FROM {ct_aeropuertos.effective_name} "
    f"ORDER BY pais, ciudad LIMIT 8"
).show(truncate=False)

# ── dim_aerolineas ────────────────────────────────────────────────────────────
_sub("dim_aerolineas → overwrite (8 aerolíneas)")
df_aerolineas = gen.aerolineas()
TableWriter(ct_aerolineas).overwrite(df_aerolineas)

spark.sql(
    f"SELECT iata_code, nombre, alianza, tipo "
    f"FROM {ct_aerolineas.effective_name} ORDER BY tipo, nombre"
).show(truncate=False)

# ── dim_tiempo ────────────────────────────────────────────────────────────────
_sub(f"dim_tiempo → overwrite ({FECHA_INICIO} → {FECHA_FIN_INIT})")
df_tiempo = gen.tiempo(FECHA_INICIO, FECHA_FIN_INIT)
TableWriter(ct_tiempo).overwrite(df_tiempo)

spark.sql(
    f"SELECT fecha, dia_semana_nombre, es_fin_semana, es_festivo "
    f"FROM {ct_tiempo.effective_name} ORDER BY fecha"
).show(truncate=False)

# ── fact_vuelos — semana inicial ──────────────────────────────────────────────
_sub(f"fact_vuelos → overwrite + append (vuelos del {FECHA_INICIO} al {FECHA_FIN_INIT})")
# fact_vuelos tiene merge_schema: true → los appends aceptan columnas nuevas en el DF
writer_fact = TableWriter(ct_fact)

fecha_actual = date.fromisoformat(FECHA_INICIO)
fecha_limite = date.fromisoformat(FECHA_FIN_INIT)
primer_dia   = True

while fecha_actual <= fecha_limite:
    fecha_str = str(fecha_actual)
    df_dia    = gen.vuelos(fecha=fecha_str, n=80)

    if primer_dia:
        writer_fact.overwrite(df_dia)
        primer_dia = False
    else:
        writer_fact.append(df_dia)

    fecha_actual += timedelta(days=1)

total = spark.sql(f"""
    SELECT COUNT(*)                AS total_vuelos,
           COUNT(DISTINCT fecha)   AS dias,
           COUNT(DISTINCT iata_aerolinea) AS aerolineas
    FROM {ct_fact.effective_name}
""").collect()[0]

print(f"\n    Bootstrap completado: {total.total_vuelos} vuelos | "
      f"{total.dias} días | {total.aerolineas} aerolíneas")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2 — Operación diaria
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 2 — Operación diaria")

# ── APPEND — vuelos del día 8 ─────────────────────────────────────────────────
_sub(f"fact_vuelos → append (vuelos nuevos {FECHA_DIA_8})")
df_dia8 = gen.vuelos_nueva_particion(fecha=FECHA_DIA_8, n=75)
writer_fact.append(df_dia8)

spark.sql(f"""
    SELECT COUNT(*) AS vuelos_dia8
    FROM {ct_fact.effective_name}
    WHERE fecha = '{FECHA_DIA_8}'
""").show()

# ── UPSERT — correcciones de vuelos del día 3 ─────────────────────────────────
fecha_corr = "2024-01-03"
_sub(f"fact_vuelos → upsert (correcciones de retrasos {fecha_corr})")
df_corr = gen.vuelos_modificados(fecha=fecha_corr, n=15)

print(f"    Retrasos ANTES de corrección ({fecha_corr}):")
spark.sql(f"""
    SELECT iata_aerolinea,
           ROUND(AVG(retraso_llegada_min), 1) AS retraso_promedio,
           COUNT(*) AS vuelos
    FROM {ct_fact.effective_name}
    WHERE fecha = '{fecha_corr}'
    GROUP BY iata_aerolinea
    ORDER BY retraso_promedio DESC
""").show()

writer_fact.upsert(
    df_corr,
    keys=["vuelo_id", "fecha"],
    update_columns=[
        "retraso_salida_min", "retraso_llegada_min",
        "hora_salida_real", "hora_llegada_real",
        "estado", "causa_retraso",
    ],
)

print(f"    Retrasos DESPUÉS de corrección ({fecha_corr}):")
spark.sql(f"""
    SELECT iata_aerolinea,
           ROUND(AVG(retraso_llegada_min), 1) AS retraso_promedio,
           COUNT(*) AS vuelos
    FROM {ct_fact.effective_name}
    WHERE fecha = '{fecha_corr}'
    GROUP BY iata_aerolinea
    ORDER BY retraso_promedio DESC
""").show()

# ── UPSERT — actualizar estado de aerolínea en dim ───────────────────────────
_sub("dim_aerolineas → upsert (Viva Air pasa a inactiva)")

schema_al = StructType([
    StructField("iata_code",  StringType(),  False),
    StructField("nombre",     StringType(),  True),
    StructField("pais_origen",StringType(),  True),
    StructField("alianza",    StringType(),  True),
    StructField("tipo",       StringType(),  True),
    StructField("activa",     BooleanType(), True),
])
df_viva = spark.createDataFrame(
    [Row("VX", "Viva Air", "Colombia", "Ninguna", "low_cost", False)],
    schema_al,
)
TableWriter(ct_aerolineas).upsert(
    df_viva,
    keys=["iata_code"],
    update_columns=["activa"],
)

spark.sql(f"""
    SELECT iata_code, nombre, activa
    FROM {ct_aerolineas.effective_name}
    WHERE iata_code = 'VX'
""").show()


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3 — Reprocesamiento de partición
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 3 — Reprocesamiento de partición")
_sub(f"fact_vuelos → overwrite_partition ({FECHA_REPROC})")

print(f"    Vuelos ANTES del reproceso ({FECHA_REPROC}):")
spark.sql(f"""
    SELECT estado, COUNT(*) AS cnt
    FROM {ct_fact.effective_name}
    WHERE fecha = '{FECHA_REPROC}'
    GROUP BY estado
""").show()

df_reproc = gen.vuelos(fecha=FECHA_REPROC, n=85)
writer_fact.overwrite_partition(df_reproc, partition={"fecha": FECHA_REPROC})

print(f"    Vuelos DESPUÉS del reproceso ({FECHA_REPROC}):")
spark.sql(f"""
    SELECT estado, COUNT(*) AS cnt
    FROM {ct_fact.effective_name}
    WHERE fecha = '{FECHA_REPROC}'
    GROUP BY estado
""").show()


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4 — Limpieza (DELETE)
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 4 — Limpieza con DELETE")
_sub("fact_vuelos → delete (vuelos con distancia_km = 0, datos corruptos)")

# Insertar vuelos corruptos para demostrar el delete
df_corrupto = spark.createDataFrame([
    ("ZZ-20240101-CORRUPT1", date(2024, 1, 1), "BOG", "BOG", "AV",
     "08:00", "08:00", "08:30", "08:30", 0, 0, 0, 0.0, 0, 0, "ON_TIME", None),
    ("ZZ-20240101-CORRUPT2", date(2024, 1, 1), "MDE", "MDE", "LA",
     "09:00", "09:00", "09:45", "09:45", 0, 0, 0, 0.0, 0, 0, "ON_TIME", None),
], gen._fact_schema())
writer_fact.append(df_corrupto)

print("    Vuelos corruptos insertados (distancia_km = 0):")
spark.sql(f"""
    SELECT vuelo_id, iata_origen, iata_destino, distancia_km
    FROM {ct_fact.effective_name}
    WHERE distancia_km = 0
""").show(truncate=False)

deleted = writer_fact.delete(
    "distancia_km = 0 OR distancia_km IS NULL",
    preview=False,
)
print(f"    Registros eliminados: {deleted}")

spark.sql(f"""
    SELECT COUNT(*) AS total_tras_delete FROM {ct_fact.effective_name}
""").show()


# ─────────────────────────────────────────────────────────────────────────────
# FASE 5 — Evolución de schema (SafeMigrator dry_run)
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 5 — Evolución de schema (SafeMigrator)")

_sub("fact_vuelos → plan de migración (dry_run=True)")
SafeMigrator(ct_fact, dry_run=True).apply()

_sub("dim_aeropuertos → plan de migración (dry_run=True)")
SafeMigrator(ct_aeropuertos, dry_run=True).apply()


# ─────────────────────────────────────────────────────────────────────────────
# FASE 6 — Validación final con queries de negocio
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 6 — Validación final")

_sub("Top 5 rutas con mayor retraso promedio")
spark.sql(f"""
    SELECT iata_origen,
           iata_destino,
           COUNT(*)                           AS vuelos,
           ROUND(AVG(retraso_llegada_min), 1) AS retraso_prom_min,
           SUM(pasajeros)                      AS total_pasajeros
    FROM {ct_fact.effective_name}
    GROUP BY iata_origen, iata_destino
    ORDER BY retraso_prom_min DESC
    LIMIT 5
""").show(truncate=False)

_sub("Ocupación por aerolínea (JOIN dim)")
spark.sql(f"""
    SELECT f.iata_aerolinea,
           a.nombre                                          AS aerolinea,
           a.alianza,
           COUNT(*)                                          AS vuelos,
           SUM(f.pasajeros)                                  AS total_pasajeros,
           ROUND(AVG(f.pasajeros * 100.0 / f.capacidad), 1)  AS ocupacion_pct
    FROM {ct_fact.effective_name} f
    JOIN {ct_aerolineas.effective_name} a ON f.iata_aerolinea = a.iata_code
    GROUP BY f.iata_aerolinea, a.nombre, a.alianza
    ORDER BY ocupacion_pct DESC
""").show(truncate=False)

_sub("Vuelos por día con distribución de estado (JOIN dim_tiempo)")
spark.sql(f"""
    SELECT t.fecha,
           t.dia_semana_nombre,
           t.es_fin_semana,
           t.es_festivo,
           COUNT(*)                                               AS total_vuelos,
           SUM(CASE WHEN f.estado = 'ON_TIME' THEN 1 ELSE 0 END)  AS a_tiempo,
           SUM(CASE WHEN f.estado = 'DELAYED' THEN 1 ELSE 0 END)   AS demorados,
           ROUND(AVG(f.retraso_llegada_min), 1)                    AS retraso_prom
    FROM {ct_fact.effective_name} f
    JOIN {ct_tiempo.effective_name} t ON f.fecha = t.fecha
    GROUP BY t.fecha, t.dia_semana_nombre, t.es_fin_semana, t.es_festivo
    ORDER BY t.fecha
""").show(20, truncate=False)

_sub("Aeropuertos más transitados (JOIN dim_aeropuertos)")
spark.sql(f"""
    SELECT ap.ciudad,
           ap.pais,
           COUNT(*) AS operaciones
    FROM (
        SELECT iata_origen AS iata FROM {ct_fact.effective_name}
        UNION ALL
        SELECT iata_destino FROM {ct_fact.effective_name}
    ) ops
    JOIN {ct_aeropuertos.effective_name} ap ON ops.iata = ap.iata_code
    GROUP BY ap.ciudad, ap.pais
    ORDER BY operaciones DESC
    LIMIT 8
""").show(truncate=False)

_sub("Resumen ejecutivo del lakehouse")
resumen = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {ct_aeropuertos.effective_name})       AS aeropuertos,
        (SELECT COUNT(*) FROM {ct_aerolineas.effective_name})        AS aerolineas,
        (SELECT COUNT(*) FROM {ct_tiempo.effective_name})            AS dias_calendario,
        (SELECT COUNT(*) FROM {ct_fact.effective_name})              AS total_vuelos,
        (SELECT COUNT(DISTINCT fecha) FROM {ct_fact.effective_name}) AS dias_con_vuelos,
        (SELECT ROUND(AVG(retraso_llegada_min), 1)
         FROM {ct_fact.effective_name})                              AS retraso_prom_global,
        (SELECT ROUND(AVG(pasajeros * 100.0 / capacidad), 1)
         FROM {ct_fact.effective_name})                              AS ocupacion_pct_global
""").collect()[0]

print(f"""
    ┌──────────────────────────────────────────────┐
    │           RESUMEN DEL LAKEHOUSE              │
    ├──────────────────────────────────────────────┤
    │  Aeropuertos en dim     : {resumen.aeropuertos:>6}             │
    │  Aerolíneas en dim      : {resumen.aerolineas:>6}             │
    │  Días en calendario     : {resumen.dias_calendario:>6}             │
    │  Total vuelos (fact)    : {resumen.total_vuelos:>6}             │
    │  Días con vuelos        : {resumen.dias_con_vuelos:>6}             │
    │  Retraso prom. global   : {resumen.retraso_prom_global:>5.1f} min         │
    │  Ocupación prom. global : {resumen.ocupacion_pct_global:>5.1f} %           │
    └──────────────────────────────────────────────┘
""")

print("\n✔ Pipeline completado exitosamente\n")