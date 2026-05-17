"""
pipeline_manufactura.py
=======================
Pipeline de manufactura de artículos de aseo — bronze → silver → gold.

Demuestra el patrón completo de un pipeline gobernado y testeado:

  FASE 1 — Bootstrap bronze
    · TableWriter.overwrite() para las 3 tablas raw con datos sintéticos sucios.

  FASE 2 — Transformaciones a silver
    · Aplica funciones puras (transformar_*_silver) a cada bronze.
    · Escribe el resultado con TableWriter.overwrite().
    · Ejecuta DQ checks declarativos sobre el resultado.
    · silver_ventas tiene distribuidor_id con mask declarado en el contrato.

  FASE 3 — Agregaciones a gold
    · Aplica funciones de KPI (kpi_*) a las tablas silver.
    · Escribe gold con TableWriter.overwrite().
    · Ejecuta DQ checks sobre los KPIs.

  FASE 4 — Reporte ejecutivo
    · Queries de negocio sobre las 3 capas.

Los UNIT TESTS de las transformaciones viven en tests/. Aquí solo
ejecutamos el pipeline end-to-end con los DQ checks.

Ejecutar:
    python3 pipeline_manufactura.py

Para correr los unit tests:
    pytest tests/ -v
"""

from __future__ import annotations

from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter

from data_generator import DataGenerator
from transformations import (
    transformar_ordenes_silver,
    transformar_lotes_silver,
    transformar_ventas_silver,
    kpi_eficiencia_planta,
    kpi_calidad_lotes,
    kpi_ventas_producto,
)
from dq import RuleSet
from dq.rules import (
    REGLAS_SILVER_ORDENES, REGLAS_SILVER_LOTES, REGLAS_SILVER_VENTAS,
    REGLAS_GOLD_EFICIENCIA, REGLAS_GOLD_CALIDAD, REGLAS_GOLD_VENTAS,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers visuales
# ─────────────────────────────────────────────────────────────────────────────

def _sep(titulo: str) -> None:
    print(f"\n{'═' * 72}")
    print(f"  {titulo}")
    print('═' * 72)


def _sub(titulo: str) -> None:
    print(f"\n  ── {titulo} {'─' * max(1, 60 - len(titulo))}")


def aplicar_dq(df, reglas_dict, fail_on_error: bool = True) -> None:
    """Ejecuta un RuleSet contra un DF y reporta. Lanza si hay errores críticos."""
    ruleset = RuleSet.from_dict(reglas_dict)
    report  = ruleset.run(df)
    report.print()
    if fail_on_error:
        report.raise_if_failed()


# ─────────────────────────────────────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────────────────────────────────────

launcher = Launcher("config/config.json")
spark    = launcher.spark
gen      = DataGenerator(spark)


# ── Cargar contratos ──────────────────────────────────────────────────────────
_sep("Cargando contratos")

# Bronze
ct_b_ordenes = load_contract("tables/bronze_ordenes_produccion_raw.json")
ct_b_lotes   = load_contract("tables/bronze_lotes_produccion_raw.json")
ct_b_ventas  = load_contract("tables/bronze_ventas_raw.json")

# Silver
ct_s_ordenes = load_contract("tables/silver_ordenes_produccion.json")
ct_s_lotes   = load_contract("tables/silver_lotes_produccion.json")
ct_s_ventas  = load_contract("tables/silver_ventas.json")

# Gold
ct_g_eficiencia = load_contract("tables/gold_eficiencia_planta.json")
ct_g_calidad    = load_contract("tables/gold_calidad_lotes.json")
ct_g_ventas     = load_contract("tables/gold_ventas_producto.json")

todos = [
    ct_b_ordenes, ct_b_lotes, ct_b_ventas,
    ct_s_ordenes, ct_s_lotes, ct_s_ventas,
    ct_g_eficiencia, ct_g_calidad, ct_g_ventas,
]
for ct in todos:
    print(f"  ✔ {ct.effective_name:60s} cols={len(ct.columns)}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 — Bootstrap bronze
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 1 — Bootstrap bronze (datos crudos sintéticos)")

_sub("Generando órdenes de producción sucias (n=200)")
df_b_ordenes = gen.ordenes_produccion(n=200)
print(f"    Filas generadas (con duplicados intencionales): {df_b_ordenes.count()}")
TableWriter(ct_b_ordenes).overwrite(df_b_ordenes)

_sub("Generando lotes de producción (n=300)")
df_b_lotes = gen.lotes_produccion(n=300)
print(f"    Filas generadas: {df_b_lotes.count()}")
TableWriter(ct_b_lotes).overwrite(df_b_lotes)

_sub("Generando ventas (n=500)")
df_b_ventas = gen.ventas(n=500)
print(f"    Filas generadas: {df_b_ventas.count()}")
TableWriter(ct_b_ventas).overwrite(df_b_ventas)

# Releemos desde el catálogo — los DataFrames en memoria no tienen las columnas
# con `default` (cargado_en, etc.) que los writers añaden internamente.
# Releer garantiza que silver se construye sobre lo realmente persistido en bronze.
df_b_ordenes = spark.read.table(ct_b_ordenes.effective_name)
df_b_lotes   = spark.read.table(ct_b_lotes.effective_name)
df_b_ventas  = spark.read.table(ct_b_ventas.effective_name)


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2 — Transformaciones bronze → silver + DQ
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 2 — Transformaciones a silver con DQ post-escritura")

# ── Órdenes ───────────────────────────────────────────────────────────────────
_sub("Órdenes producción → silver")
df_s_ordenes = transformar_ordenes_silver(df_b_ordenes)
print(f"    Bronze: {df_b_ordenes.count():>5} | Silver: {df_s_ordenes.count():>5} "
      f"(descartadas/dedupeadas: {df_b_ordenes.count() - df_s_ordenes.count()})")
TableWriter(ct_s_ordenes).overwrite(df_s_ordenes)
aplicar_dq(spark.read.table(ct_s_ordenes.effective_name), REGLAS_SILVER_ORDENES)

# ── Lotes ─────────────────────────────────────────────────────────────────────
_sub("Lotes producción → silver")
df_s_lotes = transformar_lotes_silver(df_b_lotes)
print(f"    Bronze: {df_b_lotes.count():>5} | Silver: {df_s_lotes.count():>5}")
TableWriter(ct_s_lotes).overwrite(df_s_lotes)
aplicar_dq(spark.read.table(ct_s_lotes.effective_name), REGLAS_SILVER_LOTES)

# ── Ventas ────────────────────────────────────────────────────────────────────
_sub("Ventas → silver")
df_s_ventas = transformar_ventas_silver(df_b_ventas)
print(f"    Bronze: {df_b_ventas.count():>5} | Silver: {df_s_ventas.count():>5} "
      f"(descartadas: CANCELLED y PENDING)")
TableWriter(ct_s_ventas).overwrite(df_s_ventas)
aplicar_dq(spark.read.table(ct_s_ventas.effective_name), REGLAS_SILVER_VENTAS)


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3 — Agregaciones silver → gold + DQ
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 3 — Agregaciones a gold con DQ post-escritura")

# Releemos las tablas silver desde el catálogo (no desde el DF en memoria)
# para garantizar que el gold se construye sobre lo que realmente está persistido.
df_s_ordenes_r = spark.read.table(ct_s_ordenes.effective_name)
df_s_lotes_r   = spark.read.table(ct_s_lotes.effective_name)
df_s_ventas_r  = spark.read.table(ct_s_ventas.effective_name)

# ── Eficiencia planta ─────────────────────────────────────────────────────────
_sub("KPI eficiencia planta (diaria por línea)")
df_g_eficiencia = kpi_eficiencia_planta(df_s_ordenes_r)
print(f"    Filas KPI: {df_g_eficiencia.count()}")
TableWriter(ct_g_eficiencia).overwrite(df_g_eficiencia)
aplicar_dq(spark.read.table(ct_g_eficiencia.effective_name), REGLAS_GOLD_EFICIENCIA)

# ── Calidad lotes ─────────────────────────────────────────────────────────────
_sub("KPI calidad lotes (mensual por producto)")
df_g_calidad = kpi_calidad_lotes(df_s_lotes_r)
print(f"    Filas KPI: {df_g_calidad.count()}")
TableWriter(ct_g_calidad).overwrite(df_g_calidad)
aplicar_dq(spark.read.table(ct_g_calidad.effective_name), REGLAS_GOLD_CALIDAD)

# ── Ventas producto ───────────────────────────────────────────────────────────
_sub("KPI ventas por producto (mensual con ranking)")
df_g_ventas = kpi_ventas_producto(df_s_ventas_r)
print(f"    Filas KPI: {df_g_ventas.count()}")
TableWriter(ct_g_ventas).overwrite(df_g_ventas)
aplicar_dq(spark.read.table(ct_g_ventas.effective_name), REGLAS_GOLD_VENTAS)


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4 — Reporte ejecutivo
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 4 — Reporte ejecutivo")

_sub("Top 5 productos por ingresos (último mes disponible)")
spark.sql(f"""
    WITH ultimo_mes AS (
        SELECT MAX(anio_mes) AS m FROM {ct_g_ventas.effective_name}
    )
    SELECT v.anio_mes, v.producto_id, v.unidades_netas,
           v.monto_neto, v.ranking_mes
    FROM {ct_g_ventas.effective_name} v
    JOIN ultimo_mes u ON v.anio_mes = u.m
    WHERE v.ranking_mes <= 5
    ORDER BY v.ranking_mes
""").show(truncate=False)

_sub("Eficiencia promedio por línea (todo el período)")
spark.sql(f"""
    SELECT linea_id,
           COUNT(*)                                AS dias,
           SUM(ordenes_completadas)                AS total_completadas,
           SUM(ordenes_canceladas)                 AS total_canceladas,
           ROUND(AVG(cumplimiento_pct), 2)         AS cumplimiento_prom_pct,
           ROUND(SUM(tiempo_productivo_min)/60, 1) AS horas_productivas
    FROM {ct_g_eficiencia.effective_name}
    GROUP BY linea_id
    ORDER BY linea_id
""").show(truncate=False)

_sub("Productos con peor tasa de aprobación QC")
spark.sql(f"""
    SELECT producto_id,
           ROUND(AVG(tasa_aprobacion), 2) AS tasa_aprobacion_prom,
           ROUND(AVG(merma_pct_prom), 2)   AS merma_prom,
           SUM(lotes_totales)              AS lotes_total
    FROM {ct_g_calidad.effective_name}
    GROUP BY producto_id
    ORDER BY tasa_aprobacion_prom ASC
    LIMIT 5
""").show(truncate=False)

_sub("Resumen del lakehouse")
resumen = spark.sql(f"""
    SELECT
      (SELECT COUNT(*) FROM {ct_b_ordenes.effective_name})    AS bronze_ordenes,
      (SELECT COUNT(*) FROM {ct_s_ordenes.effective_name})    AS silver_ordenes,
      (SELECT COUNT(*) FROM {ct_s_lotes.effective_name})      AS silver_lotes,
      (SELECT COUNT(*) FROM {ct_s_ventas.effective_name})     AS silver_ventas,
      (SELECT COUNT(*) FROM {ct_g_eficiencia.effective_name}) AS gold_eficiencia,
      (SELECT COUNT(*) FROM {ct_g_calidad.effective_name})    AS gold_calidad,
      (SELECT COUNT(*) FROM {ct_g_ventas.effective_name})     AS gold_ventas
""").collect()[0]

print(f"""
    ┌─────────────────────────────────────────────────┐
    │       RESUMEN DEL LAKEHOUSE — 3 CAPAS           │
    ├─────────────────────────────────────────────────┤
    │  Bronze ordenes raw       : {resumen.bronze_ordenes:>6}            │
    │  Silver ordenes (limpio)  : {resumen.silver_ordenes:>6}            │
    │  Silver lotes             : {resumen.silver_lotes:>6}            │
    │  Silver ventas            : {resumen.silver_ventas:>6}            │
    │  Gold KPI eficiencia      : {resumen.gold_eficiencia:>6}            │
    │  Gold KPI calidad         : {resumen.gold_calidad:>6}            │
    │  Gold KPI ventas producto : {resumen.gold_ventas:>6}            │
    └─────────────────────────────────────────────────┘
""")

print("\n✔ Pipeline completado exitosamente — todas las DQ checks pasaron\n")