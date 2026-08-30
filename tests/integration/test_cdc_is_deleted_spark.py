"""
test_cdc_is_deleted_spark.py — Verificación con Spark real del issue #25.

Los mocks demuestran que se llama a `coalesce`. Lo que solo se puede demostrar
con Spark de verdad es la consecuencia: que un NULL en `is_deleted` hace
desaparecer la fila de un `WHERE NOT is_deleted`, sin error alguno.

Se omite si pyspark/delta no están instalados, y también si otro módulo de test
ha mockeado pyspark en el proceso.

Ejecutar:
    pytest tests/integration -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.integration

pyspark = pytest.importorskip("pyspark", reason="requiere PySpark local")
pytest.importorskip("delta", reason="requiere delta-spark")

if isinstance(pyspark, MagicMock):
    pytest.skip(
        "pyspark está mockeado por otro módulo de test — ejecuta la "
        "integración en su propio proceso: pytest tests/integration",
        allow_module_level=True,
    )

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    sess = (
        SparkSession.builder
            .appName("DKOps-IT-IsDeleted")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
    )
    sess.sparkContext.setLogLevel("ERROR")
    yield sess
    sess.stop()


def _df(spark):
    """3 filas: una borrada, dos vigentes pero con is_deleted a NULL."""
    return spark.createDataFrame(
        [("C1", True), ("C2", None), ("C3", None)],
        "cliente_id string, is_deleted boolean",
    )


def test_null_hace_desaparecer_filas_de_where_not_is_deleted(spark):
    """
    El daño del issue #25. `NOT NULL` es NULL, no TRUE, así que las filas
    vigentes se pierden en el filtro natural — sin excepción ni aviso.
    """
    vigentes = _df(spark).filter(~F.col("is_deleted")).count()
    assert vigentes == 0, (
        "si esto no es 0, la lógica ternaria de Spark ha cambiado y el "
        "razonamiento del issue #25 habría que revisarlo"
    )


def test_coalesce_recupera_las_filas_vigentes(spark):
    """Con el relleno que aplica _ensure_is_deleted, el filtro ya funciona."""
    saneado = _df(spark).withColumn(
        "is_deleted", F.coalesce(F.col("is_deleted"), F.lit(False))
    )

    assert saneado.filter(~F.col("is_deleted")).count() == 2
    assert saneado.filter(F.col("is_deleted")).count() == 1
    assert saneado.filter(F.col("is_deleted").isNull()).count() == 0
