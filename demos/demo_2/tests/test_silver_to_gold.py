"""
test_silver_to_gold.py
======================
Unit tests sobre las transformaciones silver → gold (KPIs).

Como los KPIs son agregaciones, los tests verifican:
  · La granularidad correcta (una fila por (fecha, linea) o (mes, producto)).
  · Las sumas/promedios calculados.
  · Casos de borde: divisiones por cero, todos NULL, etc.
  · El ranking se asigna correctamente.
"""

from __future__ import annotations

from datetime import datetime, date

from pyspark.sql import types as T

from demo_2.transformations.silver_to_gold import (
    kpi_eficiencia_planta,
    kpi_calidad_lotes,
    kpi_ventas_producto,
)


# ─────────────────────────────────────────────────────────────────────────────
# Schemas silver
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_ORDENES_SILVER = T.StructType([
    T.StructField("orden_id",          T.StringType(),    False),
    T.StructField("linea_id",          T.StringType(),    False),
    T.StructField("producto_id",       T.StringType(),    False),
    T.StructField("fecha_inicio",      T.TimestampType(), False),
    T.StructField("fecha_fin",         T.TimestampType(), True),
    T.StructField("duracion_min",      T.IntegerType(),   True),
    T.StructField("cantidad_planeada", T.IntegerType(),   False),
    T.StructField("cantidad_real",     T.IntegerType(),   False),
    T.StructField("cumplimiento_pct",  T.DoubleType(),    True),
    T.StructField("estado",            T.StringType(),    False),
    T.StructField("operador",          T.StringType(),    True),
])

SCHEMA_LOTES_SILVER = T.StructType([
    T.StructField("lote_id",            T.StringType(),  False),
    T.StructField("orden_id",           T.StringType(),  False),
    T.StructField("producto_id",        T.StringType(),  False),
    T.StructField("fecha_produccion",   T.DateType(),    False),
    T.StructField("cantidad_producida", T.IntegerType(), False),
    T.StructField("cantidad_defectuosa",T.IntegerType(), False),
    T.StructField("cantidad_neta",      T.IntegerType(), True),
    T.StructField("merma_pct",          T.DoubleType(),  True),
    T.StructField("resultado_qc",       T.StringType(),  False),
    T.StructField("ph_medido",          T.DoubleType(),  True),
    T.StructField("viscosidad_cp",      T.DoubleType(),  True),
    T.StructField("ph_dentro_rango",    T.BooleanType(), True),
])

SCHEMA_VENTAS_SILVER = T.StructType([
    T.StructField("venta_id",        T.StringType(),  False),
    T.StructField("fecha",           T.DateType(),    False),
    T.StructField("distribuidor_id", T.StringType(),  False),
    T.StructField("producto_id",     T.StringType(),  False),
    T.StructField("cantidad",        T.IntegerType(), False),
    T.StructField("precio_unitario", T.DoubleType(),  False),
    T.StructField("monto_total",     T.DoubleType(),  False),
    T.StructField("es_devolucion",   T.BooleanType(), False),
])


# ═════════════════════════════════════════════════════════════════════════════
# KPI eficiencia planta
# ═════════════════════════════════════════════════════════════════════════════

class TestKpiEficienciaPlanta:

    def _row(self, orden, linea, dia, planeada, real, estado, duracion=120):
        ts = datetime(2024, 1, dia, 8, 0, 0)
        return (
            orden, linea, "PROD", ts,
            datetime(2024, 1, dia, 10, 0, 0) if estado == "COMPLETED" else None,
            duracion if estado == "COMPLETED" else None,
            planeada, real,
            (real / planeada * 100) if planeada > 0 else None,
            estado, None,
        )

    def test_agrupa_por_fecha_y_linea(self, spark):
        rows = [
            self._row("O1", "L1", 15, 1000, 950,  "COMPLETED"),
            self._row("O2", "L1", 15, 500,  500,  "COMPLETED"),
            self._row("O3", "L2", 15, 800,  800,  "COMPLETED"),
            self._row("O4", "L1", 16, 1000, 1000, "COMPLETED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_SILVER)

        result = kpi_eficiencia_planta(df).collect()

        # 3 grupos: (15, L1), (15, L2), (16, L1)
        assert len(result) == 3

        l1_dia15 = next(r for r in result if r.fecha == date(2024, 1, 15) and r.linea_id == "L1")
        assert l1_dia15.ordenes_completadas == 2
        assert l1_dia15.unidades_planeadas  == 1500
        assert l1_dia15.unidades_producidas == 1450

    def test_cumplimiento_pct_calculado(self, spark):
        rows = [
            self._row("O1", "L1", 15, 1000, 800, "COMPLETED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_SILVER)

        result = kpi_eficiencia_planta(df).collect()[0]
        assert result.cumplimiento_pct == 80.0

    def test_separa_completadas_de_canceladas(self, spark):
        rows = [
            self._row("O1", "L1", 15, 1000, 1000, "COMPLETED"),
            self._row("O2", "L1", 15, 500,  0,    "CANCELLED"),
            self._row("O3", "L1", 15, 200,  200,  "COMPLETED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_SILVER)

        result = kpi_eficiencia_planta(df).collect()[0]

        assert result.ordenes_completadas == 2
        assert result.ordenes_canceladas  == 1

    def test_tiempo_productivo_solo_cuenta_completadas(self, spark):
        rows = [
            self._row("O1", "L1", 15, 100, 100, "COMPLETED", duracion=60),
            self._row("O2", "L1", 15, 100, 0,   "CANCELLED", duracion=30),
            self._row("O3", "L1", 15, 100, 100, "COMPLETED", duracion=120),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_SILVER)

        result = kpi_eficiencia_planta(df).collect()[0]
        # Solo O1 y O3 cuentan: 60 + 120 = 180
        assert result.tiempo_productivo_min == 180


# ═════════════════════════════════════════════════════════════════════════════
# KPI calidad lotes
# ═════════════════════════════════════════════════════════════════════════════

class TestKpiCalidadLotes:

    def _row(self, lote, prod, dia_mes, qc, merma=2.0):
        return (
            lote, "ORD-X", prod, date(2024, 1, dia_mes),
            1000, int(1000 * merma / 100),                 # producida, defectuosa
            1000 - int(1000 * merma / 100),                # neta
            merma, qc, 6.5, 1500.0, True,
        )

    def test_tasa_aprobacion(self, spark):
        # 3 aprobados, 1 rechazado, 1 retest → 3/(3+1) = 75%
        rows = [
            self._row("L1", "JABON", 15, "APPROVED"),
            self._row("L2", "JABON", 16, "APPROVED"),
            self._row("L3", "JABON", 17, "APPROVED"),
            self._row("L4", "JABON", 18, "REJECTED"),
            self._row("L5", "JABON", 19, "RETEST"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_SILVER)

        result = kpi_calidad_lotes(df).collect()[0]

        assert result.lotes_totales    == 5
        assert result.lotes_aprobados  == 3
        assert result.lotes_rechazados == 1
        assert result.tasa_aprobacion  == 75.0  # 3/(3+1)

    def test_separa_por_producto(self, spark):
        rows = [
            self._row("L1", "JABON",   15, "APPROVED"),
            self._row("L2", "DETERG",  15, "REJECTED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_SILVER)

        result = kpi_calidad_lotes(df).collect()
        assert len(result) == 2

    def test_tasa_aprobacion_cero_si_solo_retests(self, spark):
        # Si solo hay RETEST (sin veredicto), denominador es 0 → tasa=0
        rows = [
            self._row("L1", "P", 15, "RETEST"),
            self._row("L2", "P", 16, "RETEST"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_SILVER)

        result = kpi_calidad_lotes(df).collect()[0]
        assert result.tasa_aprobacion == 0.0

    def test_merma_promedio(self, spark):
        rows = [
            self._row("L1", "P", 15, "APPROVED", merma=4.0),
            self._row("L2", "P", 16, "APPROVED", merma=6.0),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_SILVER)

        result = kpi_calidad_lotes(df).collect()[0]
        assert result.merma_pct_prom == 5.0  # promedio de 4 y 6


# ═════════════════════════════════════════════════════════════════════════════
# KPI ventas producto
# ═════════════════════════════════════════════════════════════════════════════

class TestKpiVentasProducto:

    def _row(self, vid, dia_mes, prod, cantidad, precio, devol=False):
        return (
            vid, date(2024, 1, dia_mes), "DIST-1", prod,
            cantidad, precio, round(cantidad * precio, 2), devol,
        )

    def test_suma_correcta(self, spark):
        rows = [
            self._row("V1", 15, "JABON", 100, 5000.0),  # 500_000
            self._row("V2", 16, "JABON",  50, 5000.0),  # 250_000
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_SILVER)

        result = kpi_ventas_producto(df).collect()[0]

        assert result.unidades_netas == 150
        assert result.monto_neto     == 750_000.0

    def test_devoluciones_restan(self, spark):
        rows = [
            self._row("V1", 15, "JABON", 100, 5000.0),                    # +500_000
            self._row("V2", 16, "JABON", -20, 5000.0, devol=True),        # -100_000
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_SILVER)

        result = kpi_ventas_producto(df).collect()[0]

        assert result.unidades_netas == 80
        assert result.monto_neto     == 400_000.0

    def test_ranking_dentro_del_mes(self, spark):
        rows = [
            self._row("V1", 15, "PROD-A", 100, 1000.0),   # 100_000
            self._row("V2", 16, "PROD-B", 200, 1000.0),   # 200_000  ← top
            self._row("V3", 17, "PROD-C",  50, 1000.0),   #  50_000
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_SILVER)

        result = {r.producto_id: r.ranking_mes for r in kpi_ventas_producto(df).collect()}

        assert result["PROD-B"] == 1
        assert result["PROD-A"] == 2
        assert result["PROD-C"] == 3

    def test_ranking_es_independiente_por_mes(self, spark):
        # Mismos productos pero en meses distintos → ranking se reinicia
        rows = [
            self._row("V1", 15, "PROD-A", 100, 1000.0),     # enero 100k
            self._row("V2", 15, "PROD-B",  50, 1000.0),     # enero  50k
            (   "V3", date(2024, 2, 15), "DIST-1", "PROD-A",  10, 1000.0,  10_000.0, False),  # feb 10k
            (   "V4", date(2024, 2, 15), "DIST-1", "PROD-B",  80, 1000.0,  80_000.0, False),  # feb 80k
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_SILVER)

        results = kpi_ventas_producto(df).collect()
        rank = {(r.anio_mes, r.producto_id): r.ranking_mes for r in results}

        assert rank[("2024-01", "PROD-A")] == 1
        assert rank[("2024-01", "PROD-B")] == 2
        assert rank[("2024-02", "PROD-B")] == 1
        assert rank[("2024-02", "PROD-A")] == 2
