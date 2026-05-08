"""
silver_to_gold.py
=================
Transformaciones silver → gold (KPIs de negocio).

Mismo principio que bronze_to_silver: funciones puras, testeables.
Cada KPI es una función que recibe los DataFrames silver y devuelve
un DataFrame con la métrica agregada.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# KPI: Eficiencia de planta (proxy OEE)
# ─────────────────────────────────────────────────────────────────────────────

def kpi_eficiencia_planta(df_ordenes: DataFrame) -> DataFrame:
    """
    Eficiencia diaria por línea de producción.

    Calcula:
      · ordenes_completadas / canceladas
      · unidades_planeadas / producidas
      · cumplimiento_pct (producidas / planeadas)
      · tiempo_productivo_min (suma de duración de órdenes COMPLETED)

    Granularidad: una fila por (fecha, linea_id).
    """
    df = df_ordenes.withColumn("fecha", F.to_date("fecha_inicio"))

    return (
        df.groupBy("fecha", "linea_id")
          .agg(
              F.sum(F.when(F.col("estado") == "COMPLETED", 1).otherwise(0))
                  .alias("ordenes_completadas"),
              F.sum(F.when(F.col("estado") == "CANCELLED", 1).otherwise(0))
                  .alias("ordenes_canceladas"),
              F.sum("cantidad_planeada").alias("unidades_planeadas"),
              F.sum("cantidad_real").alias("unidades_producidas"),
              F.sum(
                  F.when(F.col("estado") == "COMPLETED", F.col("duracion_min"))
                   .otherwise(0)
              ).alias("tiempo_productivo_min"),
          )
          .withColumn(
              "cumplimiento_pct",
              F.when(
                  F.col("unidades_planeadas") > 0,
                  F.round(F.col("unidades_producidas") / F.col("unidades_planeadas") * 100, 2),
              ).otherwise(F.lit(0.0)),
          )
          .select(
              "fecha", "linea_id",
              "ordenes_completadas", "ordenes_canceladas",
              "unidades_planeadas", "unidades_producidas", "cumplimiento_pct",
              "tiempo_productivo_min",
          )
    )


# ─────────────────────────────────────────────────────────────────────────────
# KPI: Calidad de lotes
# ─────────────────────────────────────────────────────────────────────────────

def kpi_calidad_lotes(df_lotes: DataFrame) -> DataFrame:
    """
    Tasa mensual de aprobación QC y merma promedio por producto.

    Granularidad: una fila por (anio_mes, producto_id).
    Excluye RETEST del denominador de aprobación porque aún no hay veredicto.
    """
    df = df_lotes.withColumn(
        "anio_mes",
        F.date_format("fecha_produccion", "yyyy-MM"),
    )

    return (
        df.groupBy("anio_mes", "producto_id")
          .agg(
              F.count("*").alias("lotes_totales"),
              F.sum(F.when(F.col("resultado_qc") == "APPROVED", 1).otherwise(0))
                  .alias("lotes_aprobados"),
              F.sum(F.when(F.col("resultado_qc") == "REJECTED", 1).otherwise(0))
                  .alias("lotes_rechazados"),
              F.round(F.avg("merma_pct"), 2).alias("merma_pct_prom"),
          )
          .withColumn(
              "tasa_aprobacion",
              F.when(
                  (F.col("lotes_aprobados") + F.col("lotes_rechazados")) > 0,
                  F.round(
                      F.col("lotes_aprobados") /
                      (F.col("lotes_aprobados") + F.col("lotes_rechazados")) * 100,
                      2,
                  ),
              ).otherwise(F.lit(0.0)),
          )
          .select(
              "anio_mes", "producto_id",
              "lotes_totales", "lotes_aprobados", "lotes_rechazados",
              "tasa_aprobacion", "merma_pct_prom",
          )
    )


# ─────────────────────────────────────────────────────────────────────────────
# KPI: Ventas por producto
# ─────────────────────────────────────────────────────────────────────────────

def kpi_ventas_producto(df_ventas: DataFrame) -> DataFrame:
    """
    Ventas mensuales por producto con ranking por monto neto.

    Las devoluciones (cantidad negativa) restan del total — eso es lo correcto
    para el monto neto. El ranking se calcula por mes con dense_rank.
    """
    df = df_ventas.withColumn(
        "anio_mes",
        F.date_format("fecha", "yyyy-MM"),
    )

    agregado = (
        df.groupBy("anio_mes", "producto_id")
          .agg(
              F.sum("cantidad").alias("unidades_netas"),
              F.round(F.sum("monto_total"), 2).alias("monto_neto"),
          )
    )

    w = Window.partitionBy("anio_mes").orderBy(F.col("monto_neto").desc())

    return (
        agregado
        .withColumn("ranking_mes", F.dense_rank().over(w))
        .select(
            "anio_mes", "producto_id",
            "unidades_netas", "monto_neto", "ranking_mes",
        )
    )
