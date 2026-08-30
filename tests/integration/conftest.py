"""
conftest.py — Fixtures compartidos de los tests de integración.

Una SOLA SparkSession para todo el paquete. Es obligatorio, no una
optimización: la JVM admite una única sesión activa, y parte de la
configuración —`spark.sql.warehouse.dir` entre otras— es estática y no se
puede cambiar una vez creado el SparkContext. Si cada módulo construyera la
suya, el segundo recibiría la del primero vía `getOrCreate()` y correría con
un warehouse y unas extensiones que no son las que pidió.

La sesión lleva Delta configurado aunque algún módulo no lo necesite, por la
misma razón: el primero que arranque fija las condiciones de todos.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

BASE      = "/tmp/dkops_it"
WAREHOUSE = f"{BASE}/warehouse"


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark", reason="requiere PySpark local")
    pytest.importorskip("delta", reason="requiere delta-spark")

    if isinstance(pyspark, MagicMock):
        pytest.skip(
            "pyspark está mockeado por otro módulo de test — ejecuta la "
            "integración en su propio proceso: pytest tests/integration"
        )

    from pyspark.sql import SparkSession

    shutil.rmtree(BASE, ignore_errors=True)

    sess = (
        SparkSession.builder
            .appName("DKOps-Integration")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
    )
    sess.sparkContext.setLogLevel("ERROR")
    yield sess
    sess.stop()
