"""
bronze_to_silver.py
===================
Transformaciones bronze → silver.

Funciones PURAS: reciben DataFrame(s), devuelven DataFrame.
Sin side effects, sin acceso a Spark global, sin escrituras.
Esto es lo que hace al pipeline testeable — cada función se puede
ejercitar con DataFrames pequeños construidos en memoria.

Reglas de la capa silver:
  · Sin duplicados por clave de negocio.
  · Tipos correctos (TIMESTAMP, DATE — no strings).
  · Estados normalizados a un vocabulario controlado.
  · Columnas calculadas (cumplimiento, merma, monto_total).
  · Filas con datos críticamente inválidos se descartan (no se intenta adivinar).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Órdenes de producción
# ─────────────────────────────────────────────────────────────────────────────

# Mapeo de estados crudos → estado canónico
_ESTADO_NORMALIZACION = {
    "COMPLETED":   "COMPLETED",
    "COMPLETE":    "COMPLETED",
    "FINALIZADA":  "COMPLETED",
    "OK":          "COMPLETED",
    "DONE":        "COMPLETED",
    "IN_PROGRESS": "IN_PROGRESS",
    "EN_PROCESO":  "IN_PROGRESS",
    "RUNNING":     "IN_PROGRESS",
    "CANCELLED":   "CANCELLED",
    "CANCELED":    "CANCELLED",
    "CANCELADA":   "CANCELLED",
    "ABORTED":     "CANCELLED",
}


def normalizar_estado_orden(df: DataFrame) -> DataFrame:
    """
    Normaliza la columna `estado` a un vocabulario controlado.
    Estados desconocidos quedan como NULL para que el filtro posterior los descarte.
    """
    mapping_expr = F.create_map(*[F.lit(x) for kv in _ESTADO_NORMALIZACION.items() for x in kv])
    return df.withColumn(
        "estado",
        mapping_expr[F.upper(F.trim(F.col("estado")))],
    )


def deduplicar_ordenes(df: DataFrame) -> DataFrame:
    """
    Elimina duplicados por `orden_id`. Conserva la fila con `cargado_en` más reciente
    — patrón "last-write-wins" típico para datos crudos.
    """
    w = Window.partitionBy("orden_id").orderBy(F.col("cargado_en").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .where(F.col("_rn") == 1)
          .drop("_rn")
    )


def transformar_ordenes_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Pipeline completo bronze → silver para órdenes de producción.

    Pasos:
      1. Limpiar orden_id (uppercase, trim).
      2. Parsear fechas string → TIMESTAMP.
      3. Normalizar estado.
      4. Deduplicar.
      5. Filtrar filas críticamente inválidas (sin orden_id, sin línea, sin fecha_inicio).
      6. Calcular columnas derivadas (duracion_min, cumplimiento_pct).
      7. Seleccionar y ordenar columnas según el contrato silver.
    """
    df = (
        df_bronze
        .withColumn("orden_id",    F.upper(F.trim(F.col("orden_id"))))
        .withColumn("linea_id",    F.upper(F.trim(F.col("linea_id"))))
        .withColumn("producto_id", F.upper(F.trim(F.col("producto_id"))))
        .withColumn("fecha_inicio", F.to_timestamp("fecha_inicio"))
        .withColumn("fecha_fin",    F.to_timestamp("fecha_fin"))
    )

    df = normalizar_estado_orden(df)
    df = deduplicar_ordenes(df)

    # Filtros de validez crítica — si fallan, la fila no se puede salvar
    df = df.where(
        F.col("orden_id").isNotNull()
        & (F.length(F.col("orden_id")) > 0)
        & F.col("linea_id").isNotNull()
        & F.col("producto_id").isNotNull()
        & F.col("fecha_inicio").isNotNull()
        & F.col("estado").isNotNull()
        & F.col("cantidad_planeada").isNotNull()
        & F.col("cantidad_real").isNotNull()
    )

    # Columnas calculadas
    df = df.withColumn(
        "duracion_min",
        F.when(
            F.col("fecha_fin").isNotNull(),
            (F.col("fecha_fin").cast("long") - F.col("fecha_inicio").cast("long")) / 60,
        ).cast("integer"),
    )
    df = df.withColumn(
        "cumplimiento_pct",
        F.when(
            F.col("cantidad_planeada") > 0,
            F.round(F.col("cantidad_real") / F.col("cantidad_planeada") * 100, 2),
        ),
    )

    return df.select(
        "orden_id", "linea_id", "producto_id",
        "fecha_inicio", "fecha_fin", "duracion_min",
        "cantidad_planeada", "cantidad_real", "cumplimiento_pct",
        "estado", "operador",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Lotes de producción
# ─────────────────────────────────────────────────────────────────────────────

PH_MIN = 5.5
PH_MAX = 7.5


def normalizar_resultado_qc(df: DataFrame) -> DataFrame:
    """QC vacío o inválido → REJECTED (decisión conservadora — sin certificado, no aprueba)."""
    valid = ["APPROVED", "REJECTED", "RETEST"]
    return df.withColumn(
        "resultado_qc",
        F.when(
            F.upper(F.trim(F.col("resultado_qc"))).isin(valid),
            F.upper(F.trim(F.col("resultado_qc"))),
        ).otherwise(F.lit("REJECTED")),
    )


def transformar_lotes_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Bronze → silver para lotes:
      · Parsea fecha como DATE.
      · Calcula cantidad_neta y merma_pct.
      · Normaliza QC (vacíos → REJECTED).
      · Marca pH dentro de rango.
      · Descarta filas sin lote_id u orden_id.
    """
    df = (
        df_bronze
        .withColumn("lote_id",      F.upper(F.trim(F.col("lote_id"))))
        .withColumn("orden_id",     F.upper(F.trim(F.col("orden_id"))))
        .withColumn("producto_id",  F.upper(F.trim(F.col("producto_id"))))
        .withColumn("fecha_produccion", F.to_date("fecha_produccion"))
    )

    df = normalizar_resultado_qc(df)

    df = df.where(
        F.col("lote_id").isNotNull()
        & (F.length(F.col("lote_id")) > 0)
        & F.col("orden_id").isNotNull()
        & F.col("producto_id").isNotNull()
        & F.col("fecha_produccion").isNotNull()
        & F.col("cantidad_producida").isNotNull()
        & F.col("cantidad_defectuosa").isNotNull()
        & (F.col("cantidad_producida") >= 0)
        & (F.col("cantidad_defectuosa") >= 0)
        & (F.col("cantidad_defectuosa") <= F.col("cantidad_producida"))
    )

    df = df.withColumn(
        "cantidad_neta",
        F.col("cantidad_producida") - F.col("cantidad_defectuosa"),
    )
    df = df.withColumn(
        "merma_pct",
        F.when(
            F.col("cantidad_producida") > 0,
            F.round(F.col("cantidad_defectuosa") / F.col("cantidad_producida") * 100, 2),
        ).otherwise(F.lit(0.0)),
    )
    df = df.withColumn(
        "ph_dentro_rango",
        F.when(
            F.col("ph_medido").isNotNull(),
            (F.col("ph_medido") >= PH_MIN) & (F.col("ph_medido") <= PH_MAX),
        ).otherwise(F.lit(False)),
    )

    return df.select(
        "lote_id", "orden_id", "producto_id", "fecha_produccion",
        "cantidad_producida", "cantidad_defectuosa", "cantidad_neta",
        "merma_pct", "resultado_qc", "ph_medido", "viscosidad_cp",
        "ph_dentro_rango",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ventas
# ─────────────────────────────────────────────────────────────────────────────

def transformar_ventas_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Bronze → silver para ventas:
      · Excluye CANCELLED y PENDING.
      · Calcula monto_total = cantidad * precio_unitario.
      · Marca devoluciones (cantidad < 0 o estado = RETURNED).
      · Filtra precios <= 0 (datos corruptos).
    """
    df = (
        df_bronze
        .withColumn("venta_id",         F.upper(F.trim(F.col("venta_id"))))
        .withColumn("distribuidor_id",  F.upper(F.trim(F.col("distribuidor_id"))))
        .withColumn("producto_id",      F.upper(F.trim(F.col("producto_id"))))
        .withColumn("estado_venta",     F.upper(F.trim(F.col("estado_venta"))))
        .withColumn("fecha",            F.to_date("fecha"))
    )

    # Solo confirmadas y devoluciones — el resto se descarta
    df = df.where(F.col("estado_venta").isin("CONFIRMED", "RETURNED"))

    df = df.where(
        F.col("venta_id").isNotNull()
        & F.col("fecha").isNotNull()
        & F.col("distribuidor_id").isNotNull()
        & F.col("producto_id").isNotNull()
        & F.col("cantidad").isNotNull()
        & F.col("precio_unitario").isNotNull()
        & (F.col("precio_unitario") > 0)
    )

    df = df.withColumn(
        "monto_total",
        F.round(F.col("cantidad") * F.col("precio_unitario"), 2),
    )
    df = df.withColumn(
        "es_devolucion",
        (F.col("cantidad") < 0) | (F.col("estado_venta") == "RETURNED"),
    )

    return df.select(
        "venta_id", "fecha", "distribuidor_id", "producto_id",
        "cantidad", "precio_unitario", "monto_total", "es_devolucion",
    )
