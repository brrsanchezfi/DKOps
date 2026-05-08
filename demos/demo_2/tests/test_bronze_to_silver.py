"""
test_bronze_to_silver.py
========================
Unit tests sobre las transformaciones bronze → silver.

Patrón general:
  1. Construir un DataFrame pequeño en memoria con datos representativos
     (incluyendo casos sucios: nulos, duplicados, casing).
  2. Ejecutar la función pura.
  3. Aserciones sobre el resultado: filas, valores, tipos.

Cada test es INDEPENDIENTE — no comparte estado con otros tests.
Los datos siempre se construyen a mano para que el test documente
exactamente qué entrada produce qué salida.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import types as T

from demo_2.transformations.bronze_to_silver import (
    transformar_ordenes_silver,
    transformar_lotes_silver,
    transformar_ventas_silver,
    normalizar_estado_orden,
    normalizar_resultado_qc,
    deduplicar_ordenes,
)


# ─────────────────────────────────────────────────────────────────────────────
# Schemas helper para construir DataFrames bronze
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_ORDENES_BRONZE = T.StructType([
    T.StructField("orden_id",          T.StringType(),  True),
    T.StructField("linea_id",          T.StringType(),  True),
    T.StructField("producto_id",       T.StringType(),  True),
    T.StructField("fecha_inicio",      T.StringType(),  True),
    T.StructField("fecha_fin",         T.StringType(),  True),
    T.StructField("cantidad_planeada", T.IntegerType(), True),
    T.StructField("cantidad_real",     T.IntegerType(), True),
    T.StructField("estado",            T.StringType(),  True),
    T.StructField("operador",          T.StringType(),  True),
    T.StructField("cargado_en",        T.TimestampType(), True),
])

SCHEMA_LOTES_BRONZE = T.StructType([
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

SCHEMA_VENTAS_BRONZE = T.StructType([
    T.StructField("venta_id",        T.StringType(),  True),
    T.StructField("fecha",           T.StringType(),  True),
    T.StructField("distribuidor_id", T.StringType(),  True),
    T.StructField("producto_id",     T.StringType(),  True),
    T.StructField("cantidad",        T.IntegerType(), True),
    T.StructField("precio_unitario", T.DoubleType(),  True),
    T.StructField("estado_venta",    T.StringType(),  True),
])


# ═════════════════════════════════════════════════════════════════════════════
# normalizar_estado_orden
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizarEstadoOrden:

    def test_estados_completados_se_unifican(self, spark):
        # Distintas formas de "completed" deben colapsar a COMPLETED
        rows = [(s,) for s in ["COMPLETED", "completed", "OK", "FINALIZADA", "Done"]]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("estado", T.StringType())]))

        result = normalizar_estado_orden(df).collect()
        estados = [r.estado for r in result]

        assert estados == ["COMPLETED"] * 5

    def test_estado_desconocido_queda_null(self, spark):
        # Estados que no están en el mapeo → NULL → la fila se descarta después
        rows = [("???",), ("",), ("UNKNOWN",), (None,)]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("estado", T.StringType())]))

        result = normalizar_estado_orden(df).collect()
        estados = [r.estado for r in result]

        assert all(e is None for e in estados), f"Esperaba todos None, obtuve {estados}"

    def test_estados_cancelados(self, spark):
        rows = [(s,) for s in ["CANCELLED", "Cancelled", "CANCELADA", "ABORTED"]]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("estado", T.StringType())]))

        estados = [r.estado for r in normalizar_estado_orden(df).collect()]
        assert estados == ["CANCELLED"] * 4


# ═════════════════════════════════════════════════════════════════════════════
# deduplicar_ordenes
# ═════════════════════════════════════════════════════════════════════════════

class TestDeduplicarOrdenes:

    def test_conserva_la_carga_mas_reciente(self, spark):
        # Misma orden_id con dos cargados_en distintos → conserva la más nueva
        ts_old = datetime(2024, 1, 1, 10, 0, 0)
        ts_new = datetime(2024, 1, 1, 11, 0, 0)

        rows = [
            ("ORD-001", "L1", "PROD-A", "2024-01-01 08:00", None, 100, 95,  "COMPLETED", "Ana", ts_old),
            ("ORD-001", "L1", "PROD-A", "2024-01-01 08:00", None, 100, 100, "COMPLETED", "Ana", ts_new),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = deduplicar_ordenes(df).collect()

        assert len(result) == 1
        assert result[0].cantidad_real == 100      # gana la fila con ts_new
        assert result[0].cargado_en   == ts_new

    def test_sin_duplicados_no_modifica(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "PROD-A", "2024-01-01", None, 100, 95, "COMPLETED", "Ana", ts),
            ("ORD-002", "L2", "PROD-B", "2024-01-02", None, 200, 200, "COMPLETED", "Pedro", ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        assert deduplicar_ordenes(df).count() == 2


# ═════════════════════════════════════════════════════════════════════════════
# transformar_ordenes_silver — pipeline completo
# ═════════════════════════════════════════════════════════════════════════════

class TestTransformarOrdenesSilver:

    def test_caso_feliz(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "JABON-1L", "2024-01-15 08:00:00",
             "2024-01-15 12:00:00", 1000, 950, "COMPLETED", "Ana", ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()

        assert len(result) == 1
        r = result[0]
        assert r.orden_id          == "ORD-001"
        assert r.estado            == "COMPLETED"
        assert r.duracion_min      == 240   # 4 horas
        assert r.cumplimiento_pct  == 95.0
        assert isinstance(r.fecha_inicio, datetime)

    def test_normaliza_casing_y_espacios(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("  ord-001  ", " l1 ", " jabon-1l ", "2024-01-15 08:00:00",
             None, 1000, 800, "completed", "Ana", ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()
        r = result[0]

        assert r.orden_id    == "ORD-001"
        assert r.linea_id    == "L1"
        assert r.producto_id == "JABON-1L"
        assert r.estado      == "COMPLETED"

    def test_descarta_filas_sin_orden_id(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "PROD-A", "2024-01-15 08:00:00", None, 100, 95, "COMPLETED", "Ana", ts),
            (None,      "L1", "PROD-A", "2024-01-15 08:00:00", None, 100, 95, "COMPLETED", "Ana", ts),
            ("",        "L1", "PROD-A", "2024-01-15 08:00:00", None, 100, 95, "COMPLETED", "Ana", ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()
        # Solo la primera sobrevive
        assert len(result) == 1
        assert result[0].orden_id == "ORD-001"

    def test_descarta_estado_invalido(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "P-A", "2024-01-15 08:00:00", None, 100, 95, "COMPLETED", "Ana", ts),
            ("ORD-002", "L1", "P-A", "2024-01-15 08:00:00", None, 100, 95, "FOO_BAR",   "Ana", ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()
        ids = [r.orden_id for r in result]

        assert "ORD-001" in ids
        assert "ORD-002" not in ids

    def test_calcula_cumplimiento_correctamente(self, spark):
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "P", "2024-01-15 08:00:00", None, 1000, 1100, "COMPLETED", None, ts),  # 110%
            ("ORD-002", "L1", "P", "2024-01-15 08:00:00", None, 1000, 0,    "COMPLETED", None, ts),  # 0%
            ("ORD-003", "L1", "P", "2024-01-15 08:00:00", None, 1000, 1000, "COMPLETED", None, ts),  # 100%
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = {r.orden_id: r.cumplimiento_pct for r in transformar_ordenes_silver(df).collect()}

        assert result["ORD-001"] == 110.0
        assert result["ORD-002"] == 0.0
        assert result["ORD-003"] == 100.0

    def test_cumplimiento_null_si_planeada_es_cero(self, spark):
        # Edge case: división por cero protegida → null (no excepción)
        ts = datetime(2024, 1, 1, 10, 0, 0)
        rows = [
            ("ORD-001", "L1", "P", "2024-01-15 08:00:00", None, 0, 0, "COMPLETED", None, ts),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()
        assert result[0].cumplimiento_pct is None

    def test_dedupe_se_aplica(self, spark):
        ts1 = datetime(2024, 1, 1, 10, 0, 0)
        ts2 = datetime(2024, 1, 1, 11, 0, 0)
        rows = [
            ("ORD-001", "L1", "P", "2024-01-15 08:00:00", None, 100, 50,  "COMPLETED", "Ana", ts1),
            ("ORD-001", "L1", "P", "2024-01-15 08:00:00", None, 100, 100, "COMPLETED", "Ana", ts2),
        ]
        df = spark.createDataFrame(rows, SCHEMA_ORDENES_BRONZE)

        result = transformar_ordenes_silver(df).collect()
        assert len(result) == 1
        assert result[0].cantidad_real == 100  # ganó la fila con ts2


# ═════════════════════════════════════════════════════════════════════════════
# normalizar_resultado_qc
# ═════════════════════════════════════════════════════════════════════════════

class TestNormalizarResultadoQC:

    def test_qc_vacio_se_marca_rejected(self, spark):
        # Decisión conservadora — sin certificado, no aprueba
        rows = [("",), (None,), ("???",)]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("resultado_qc", T.StringType())]))

        result = [r.resultado_qc for r in normalizar_resultado_qc(df).collect()]
        assert result == ["REJECTED"] * 3

    def test_qc_validos_se_normalizan(self, spark):
        rows = [(" approved ",), ("REJECTED",), ("retest",)]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("resultado_qc", T.StringType())]))

        result = [r.resultado_qc for r in normalizar_resultado_qc(df).collect()]
        assert result == ["APPROVED", "REJECTED", "RETEST"]


# ═════════════════════════════════════════════════════════════════════════════
# transformar_lotes_silver
# ═════════════════════════════════════════════════════════════════════════════

class TestTransformarLotesSilver:

    def test_caso_feliz_calcula_merma(self, spark):
        rows = [
            ("LOTE-001", "ORD-001", "JABON", "2024-01-15", 1000, 50, "APPROVED", 6.5, 1500.0),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_BRONZE)

        result = transformar_lotes_silver(df).collect()[0]

        assert result.cantidad_neta    == 950
        assert result.merma_pct        == 5.0
        assert result.ph_dentro_rango  is True

    def test_descarta_defectuosa_mayor_que_producida(self, spark):
        # Dato inválido — no se puede tener más defectuosos que producidos
        rows = [
            ("LOTE-001", "ORD-001", "P", "2024-01-15", 100, 150, "APPROVED", 6.5, 1500.0),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_BRONZE)

        assert transformar_lotes_silver(df).count() == 0

    def test_ph_fuera_de_rango(self, spark):
        rows = [
            ("LOTE-001", "ORD-001", "P", "2024-01-15", 100, 5, "APPROVED", 4.0, 1500.0),  # ph=4 < 5.5
            ("LOTE-002", "ORD-002", "P", "2024-01-15", 100, 5, "APPROVED", 8.0, 1500.0),  # ph=8 > 7.5
            ("LOTE-003", "ORD-003", "P", "2024-01-15", 100, 5, "APPROVED", 6.5, 1500.0),  # ok
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_BRONZE)

        result = {r.lote_id: r.ph_dentro_rango for r in transformar_lotes_silver(df).collect()}

        assert result["LOTE-001"] is False
        assert result["LOTE-002"] is False
        assert result["LOTE-003"] is True

    def test_qc_vacio_se_convierte_en_rejected(self, spark):
        rows = [
            ("LOTE-001", "ORD-001", "P", "2024-01-15", 100, 5, "",   6.5, 1500.0),
            ("LOTE-002", "ORD-002", "P", "2024-01-15", 100, 5, None, 6.5, 1500.0),
        ]
        df = spark.createDataFrame(rows, SCHEMA_LOTES_BRONZE)

        result = [r.resultado_qc for r in transformar_lotes_silver(df).collect()]
        assert all(r == "REJECTED" for r in result)


# ═════════════════════════════════════════════════════════════════════════════
# transformar_ventas_silver
# ═════════════════════════════════════════════════════════════════════════════

class TestTransformarVentasSilver:

    def test_excluye_canceladas_y_pendientes(self, spark):
        rows = [
            ("VTA-001", "2024-01-15", "DIST-1", "PROD", 100, 10000.0, "CONFIRMED"),
            ("VTA-002", "2024-01-15", "DIST-1", "PROD", 100, 10000.0, "CANCELLED"),
            ("VTA-003", "2024-01-15", "DIST-1", "PROD", 100, 10000.0, "PENDING"),
            ("VTA-004", "2024-01-15", "DIST-1", "PROD", -50, 10000.0, "RETURNED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_BRONZE)

        result = transformar_ventas_silver(df).collect()
        ids = sorted(r.venta_id for r in result)

        assert ids == ["VTA-001", "VTA-004"]

    def test_calcula_monto_total(self, spark):
        rows = [
            ("VTA-001", "2024-01-15", "DIST-1", "PROD", 10, 5000.0, "CONFIRMED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_BRONZE)

        result = transformar_ventas_silver(df).collect()[0]
        assert result.monto_total == 50000.0

    def test_marca_devolucion_correctamente(self, spark):
        rows = [
            ("VTA-001", "2024-01-15", "D", "P",  100, 1000.0, "CONFIRMED"),  # venta normal
            ("VTA-002", "2024-01-15", "D", "P", -50,  1000.0, "RETURNED"),   # devolución
            ("VTA-003", "2024-01-15", "D", "P", -10,  1000.0, "CONFIRMED"),  # cantidad negativa
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_BRONZE)

        result = {r.venta_id: r.es_devolucion for r in transformar_ventas_silver(df).collect()}

        assert result["VTA-001"] is False
        assert result["VTA-002"] is True
        assert result["VTA-003"] is True

    def test_descarta_precio_no_positivo(self, spark):
        rows = [
            ("VTA-001", "2024-01-15", "D", "P", 10, 1000.0, "CONFIRMED"),
            ("VTA-002", "2024-01-15", "D", "P", 10,    0.0, "CONFIRMED"),
            ("VTA-003", "2024-01-15", "D", "P", 10, -100.0, "CONFIRMED"),
        ]
        df = spark.createDataFrame(rows, SCHEMA_VENTAS_BRONZE)

        ids = sorted(r.venta_id for r in transformar_ventas_silver(df).collect())
        assert ids == ["VTA-001"]
