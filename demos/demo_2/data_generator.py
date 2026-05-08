"""
data_generator.py
=================
Genera datos sintéticos REALISTAMENTE SUCIOS para la capa bronze:
casing inconsistente, duplicados, nulos, fechas mal formateadas,
estados con sinónimos, montos inválidos.

El objetivo es que las transformaciones bronze→silver tengan que
limpiar/normalizar/deduplicar — eso es lo que se va a testear.

Uso
---
    gen = DataGenerator(spark)
    df_ordenes = gen.ordenes_produccion(n=200)
    df_lotes   = gen.lotes_produccion(n=300)
    df_ventas  = gen.ventas(n=500)
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


# ── Catálogo de productos ─────────────────────────────────────────────────────
PRODUCTOS = [
    "JABON-LIQ-1L",   "JABON-BAR-100G", "JABON-BAR-200G",
    "DET-POLVO-2KG",  "DET-LIQ-3L",     "DET-LIQ-5L",
    "SHAMP-500ML",    "SHAMP-1L",       "ACOND-500ML",
    "SUAV-1L",        "SUAV-3L",        "LIMP-MULTI-1L",
]

LINEAS = ["L1", "L2", "L3", "L4"]

DISTRIBUIDORES = [
    "DIST-001", "DIST-002", "DIST-003", "DIST-004",
    "DIST-005", "DIST-006", "DIST-007", "DIST-008",
]

OPERADORES = [
    "Carlos Mejía", "Ana Rodríguez", "Pedro Gómez",
    "Sofía Vélez",  "Juan Pérez",    "María López",
    None, None,  # 25% null para simular datos faltantes
]

# Estados de orden: variantes "sucias" que la transformación debe normalizar
ESTADOS_ORDEN_RAW = [
    "COMPLETED", "completed", "Complete", "OK", "Done", "FINALIZADA",
    "IN_PROGRESS", "in_progress", "EN_PROCESO", "running",
    "CANCELLED", "Cancelled", "CANCELADA", "ABORTED",
    "??", "", None,  # invalid → quedan NULL → se descartan
]

ESTADOS_VENTA = ["CONFIRMED", "CANCELLED", "RETURNED", "PENDING"]


def _aleatorio_o_nulo(values: list, prob_null: float = 0.0):
    if prob_null > 0 and random.random() < prob_null:
        return None
    return random.choice(values)


def _formato_fecha_aleatorio(dt: datetime) -> str:
    """Devuelve la fecha con uno de varios formatos para simular dato sucio."""
    formatos = [
        dt.strftime("%Y-%m-%d %H:%M:%S"),
        dt.strftime("%Y-%m-%dT%H:%M:%S"),
        dt.strftime("%Y-%m-%d %H:%M"),
    ]
    return random.choice(formatos)


# ─────────────────────────────────────────────────────────────────────────────

class DataGenerator:
    """
    Genera DataFrames bronze con datos sintéticos sucios.
    El seed es fijo por defecto para que el demo sea reproducible —
    si quieres aleatoriedad real, pasa seed=None.
    """

    def __init__(self, spark: SparkSession, seed: int | None = 42) -> None:
        self.spark = spark
        if seed is not None:
            random.seed(seed)

    # ── Órdenes de producción ─────────────────────────────────────────────

    def ordenes_produccion(
        self,
        n:            int = 200,
        fecha_inicio: str = "2024-01-01",
        dias:         int = 30,
    ) -> DataFrame:
        base = datetime.fromisoformat(fecha_inicio)
        rows = []

        for i in range(n):
            offset_dias  = random.randint(0, dias - 1)
            offset_horas = random.randint(0, 23)
            inicio = base + timedelta(days=offset_dias, hours=offset_horas)

            estado_raw = random.choice(ESTADOS_ORDEN_RAW)
            es_completed = estado_raw and estado_raw.upper() in {
                "COMPLETED", "COMPLETE", "OK", "DONE", "FINALIZADA"
            }

            fin = inicio + timedelta(minutes=random.randint(60, 480)) if es_completed else None

            # Casing intencionalmente inconsistente
            orden_id = random.choice([
                f"ORD-{i:05d}",
                f"  ord-{i:05d}  ",
                f"ord-{i:05d}",
            ])

            cantidad_planeada = random.randint(500, 5000)
            # Cumplimiento real con algo de varianza
            ratio = random.gauss(0.95, 0.15)
            cantidad_real = max(0, int(cantidad_planeada * ratio))

            rows.append((
                orden_id,
                random.choice(LINEAS),
                random.choice(PRODUCTOS),
                _formato_fecha_aleatorio(inicio),
                _formato_fecha_aleatorio(fin) if fin else None,
                cantidad_planeada,
                cantidad_real,
                estado_raw,
                _aleatorio_o_nulo(OPERADORES),
            ))

        # Inyectamos ~5% de duplicados a propósito
        n_dups = max(1, n // 20)
        for _ in range(n_dups):
            rows.append(random.choice(rows))

        schema = T.StructType([
            T.StructField("orden_id",          T.StringType(),  True),
            T.StructField("linea_id",          T.StringType(),  True),
            T.StructField("producto_id",       T.StringType(),  True),
            T.StructField("fecha_inicio",      T.StringType(),  True),
            T.StructField("fecha_fin",         T.StringType(),  True),
            T.StructField("cantidad_planeada", T.IntegerType(), True),
            T.StructField("cantidad_real",     T.IntegerType(), True),
            T.StructField("estado",            T.StringType(),  True),
            T.StructField("operador",          T.StringType(),  True),
        ])
        return self.spark.createDataFrame(rows, schema)

    # ── Lotes de producción ───────────────────────────────────────────────

    def lotes_produccion(
        self,
        n:            int = 300,
        fecha_inicio: str = "2024-01-01",
        dias:         int = 30,
    ) -> DataFrame:
        base = date.fromisoformat(fecha_inicio)
        rows = []

        for i in range(n):
            fecha = base + timedelta(days=random.randint(0, dias - 1))

            producida   = random.randint(100, 2000)
            # Merma típica entre 0% y 8%, con ocasional outlier
            tasa_merma  = random.choice([random.uniform(0, 0.08)] * 9 + [random.uniform(0.1, 0.3)])
            defectuosa  = int(producida * tasa_merma)

            # Resultado QC correlacionado con merma — pero con ruido
            if defectuosa / producida > 0.10:
                qc = random.choice(["REJECTED", "REJECTED", "RETEST"])
            else:
                qc = random.choice(["APPROVED", "APPROVED", "APPROVED", "RETEST", ""])

            # pH dentro o fuera de rango aleatoriamente
            ph = round(random.gauss(6.5, 0.8), 2)

            rows.append((
                f"LOTE-{i:06d}",
                f"ORD-{random.randint(0, 199):05d}",
                random.choice(PRODUCTOS),
                fecha.isoformat(),
                producida,
                defectuosa,
                qc,
                ph,
                round(random.uniform(800, 2500), 1),  # viscosidad
            ))

        schema = T.StructType([
            T.StructField("lote_id",             T.StringType(),  True),
            T.StructField("orden_id",            T.StringType(),  True),
            T.StructField("producto_id",         T.StringType(),  True),
            T.StructField("fecha_produccion",    T.StringType(),  True),
            T.StructField("cantidad_producida",  T.IntegerType(), True),
            T.StructField("cantidad_defectuosa", T.IntegerType(), True),
            T.StructField("resultado_qc",        T.StringType(),  True),
            T.StructField("ph_medido",           T.DoubleType(),  True),
            T.StructField("viscosidad_cp",       T.DoubleType(),  True),
        ])
        return self.spark.createDataFrame(rows, schema)

    # ── Ventas ────────────────────────────────────────────────────────────

    def ventas(
        self,
        n:            int = 500,
        fecha_inicio: str = "2024-01-01",
        dias:         int = 30,
    ) -> DataFrame:
        base = date.fromisoformat(fecha_inicio)
        rows = []

        for i in range(n):
            fecha = base + timedelta(days=random.randint(0, dias - 1))

            estado = random.choices(
                ESTADOS_VENTA,
                weights=[80, 5, 8, 7],  # 80% confirmadas, resto resto
            )[0]

            cantidad = random.randint(10, 500)
            if estado == "RETURNED":
                cantidad = -cantidad  # devolución

            precio = round(random.uniform(5_000, 45_000), 2)

            rows.append((
                f"VTA-{i:06d}",
                fecha.isoformat(),
                random.choice(DISTRIBUIDORES),
                random.choice(PRODUCTOS),
                cantidad,
                precio,
                estado,
            ))

        schema = T.StructType([
            T.StructField("venta_id",        T.StringType(),  True),
            T.StructField("fecha",           T.StringType(),  True),
            T.StructField("distribuidor_id", T.StringType(),  True),
            T.StructField("producto_id",     T.StringType(),  True),
            T.StructField("cantidad",        T.IntegerType(), True),
            T.StructField("precio_unitario", T.DoubleType(),  True),
            T.StructField("estado_venta",    T.StringType(),  True),
        ])
        return self.spark.createDataFrame(rows, schema)