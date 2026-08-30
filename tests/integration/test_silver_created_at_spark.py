"""
test_silver_created_at_spark.py — Verificación con Spark + Delta reales.

Cubre el punto que los mocks no pueden demostrar del issue #19: que
`_silver_created_at` **sobrevive** a un segundo MERGE sobre la misma clave,
mientras `_silver_modified_at` sí se actualiza.

Se omite automáticamente si pyspark/delta no están instalados, de modo que la
suite normal (basada en mocks) sigue corriendo en cualquier entorno.

Ejecutar:
    pytest tests/integration -v
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration

pyspark = pytest.importorskip("pyspark", reason="requiere PySpark local")
pytest.importorskip("delta", reason="requiere delta-spark")

# Los módulos de test basados en mocks registran un MagicMock bajo "pyspark"
# en sys.modules. Si este módulo se colase en el mismo proceso, hablaríamos con
# ese mock en vez de con Spark y las aserciones no probarían nada.
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

from pyspark.sql import functions as F

from DKOps.table_governance.contracts.loader import ColumnContract, TableContract
from DKOps.table_governance.writers.upsert_writer import UpsertWriter


WAREHOUSE = "/tmp/dkops_it/warehouse"   # definido en conftest.py


@pytest.fixture(scope="module")
def contract() -> TableContract:
    return TableContract(
        catalog = "ct_silver_dev",
        schema  = "it_silver",
        name    = "ventas_current",
        type    = "MANAGED",
        format  = "DELTA",
        comment = "Ventas curadas — test de integración",
        columns = (
            ColumnContract(name="venta_id", type="STRING",  nullable=False,
                           comment="Clave de negocio"),
            ColumnContract(name="importe",  type="DOUBLE",  nullable=True,
                           comment="Importe de la venta"),
            ColumnContract(name="_silver_created_at",  type="TIMESTAMP", nullable=True,
                           comment="Primera escritura en Silver"),
            ColumnContract(name="_silver_modified_at", type="TIMESTAMP", nullable=True,
                           comment="Ultima actualizacion en Silver"),
        ),
    )


def _launcher(spark):
    lc = MagicMock()
    lc.spark = spark
    lc.env   = MagicMock()
    lc.env._is_databricks = False
    lc.env.env = "dev"
    return lc


def _upsert(spark, contract, rows):
    """Ejecuta un MERGE con _silver_created_at protegido, como hacen las estrategias."""
    df = (
        spark.createDataFrame(rows, "venta_id string, importe double")
        .withColumn("_silver_created_at",  F.current_timestamp())
        .withColumn("_silver_modified_at", F.current_timestamp())
    )
    with patch(
        "DKOps.table_governance.writers.base_writer.Launcher",
        **{"current.return_value": _launcher(spark)},
    ):
        UpsertWriter(contract).write(
            df,
            merge_keys          = ["venta_id"],
            insert_only_columns = ["_silver_created_at"],
        )


def _read(spark, contract):
    path = f"{WAREHOUSE}/{contract.schema}/{contract.name}"
    return {
        r["venta_id"]: r
        for r in spark.read.format("delta").load(path).collect()
    }


def test_created_at_se_preserva_entre_merges(spark, contract):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {contract.schema}")

    # 1ª carga — la tabla no existe: carga inicial
    _upsert(spark, contract, [("V1", 100.0), ("V2", 200.0)])
    primera = _read(spark, contract)
    assert set(primera) == {"V1", "V2"}

    time.sleep(1.1)  # separar los timestamps de forma observable

    # 2ª carga — V1 cambia de importe, V3 es nueva
    _upsert(spark, contract, [("V1", 999.0), ("V3", 300.0)])
    segunda = _read(spark, contract)

    # V1: actualizada — created_at intacto, modified_at más reciente
    assert segunda["V1"]["importe"] == 999.0
    assert segunda["V1"]["_silver_created_at"] == primera["V1"]["_silver_created_at"], (
        "_silver_created_at NO debe reescribirse en un UPDATE"
    )
    assert segunda["V1"]["_silver_modified_at"] > primera["V1"]["_silver_modified_at"]

    # V2: intacta — no venía en el segundo lote
    assert segunda["V2"]["_silver_created_at"] == primera["V2"]["_silver_created_at"]

    # V3: nueva — created_at posterior al de las filas originales
    assert segunda["V3"]["_silver_created_at"] > primera["V1"]["_silver_created_at"]


def test_apply_contract_metadata_documenta_la_tabla(spark, contract):
    """Issue #18 — la carga inicial de UpsertWriter deja la tabla documentada."""
    with patch(
        "DKOps.table_governance.writers.base_writer.Launcher",
        **{"current.return_value": _launcher(spark)},
    ):
        UpsertWriter(contract).apply_contract_metadata()

    comentarios = {
        r["col_name"].strip(): (r["comment"] or "").strip()
        for r in spark.sql(f"DESCRIBE TABLE {contract.schema}.{contract.name}").collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    }
    assert comentarios.get("venta_id") == "Clave de negocio"
    assert comentarios.get("importe")  == "Importe de la venta"
