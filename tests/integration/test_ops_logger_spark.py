"""
test_ops_logger_spark.py — Verificación con Spark y Delta reales del issue #28.

Es el test que faltaba. Los de mocks comprueban que `log_success` arma la fila
correcta, pero `createDataFrame` sobre un MagicMock nunca falla: con el esquema
roto —`started_at` declarado `nullable=False`— seguían pasando en verde
mientras en producción la tabla de control acumulaba 25 filas, todas STARTED.

Aquí se escribe de verdad y se lee de vuelta.

Ejecutar:
    pytest tests/integration -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def ops(spark, tmp_path):
    from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
    return IngestionOpsLogger(
        spark    = spark,
        ops_path = str(tmp_path / "ops_control"),
        pipeline = "it_pipeline",
    )


def _filas(ops):
    return {
        (r["run_id"], r["status"]): r
        for r in ops.read().collect()
    }


# ─────────────────────────────────────────────────────────────────────────────
# El fallo del issue #28: el cierre nunca llegaba a la tabla
# ─────────────────────────────────────────────────────────────────────────────

def test_una_ejecucion_deja_apertura_y_cierre(ops):
    run_id = ops.log_start("ventas")
    ops.log_success(run_id, "ventas", rows_read=120, rows_written=100)

    filas = _filas(ops)
    assert len(filas) == 2, (
        f"Se esperaban STARTED y SUCCESS; hay {sorted(k[1] for k in filas)}"
    )
    assert (run_id, "STARTED") in filas
    assert (run_id, "SUCCESS") in filas


def test_el_cierre_guarda_las_filas_escritas(ops):
    run_id = ops.log_start("ventas")
    ops.log_success(run_id, "ventas", rows_read=120, rows_written=100)

    cierre = _filas(ops)[(run_id, "SUCCESS")]
    assert cierre["rows_read"]    == 120
    assert cierre["rows_written"] == 100


def test_un_fallo_deja_rastro_en_la_tabla(ops):
    """Es justo cuando mas se necesita el registro operativo."""
    run_id = ops.log_start("ventas")
    ops.log_failure(run_id, "ventas", ValueError("origen vacio"))

    cierre = _filas(ops)[(run_id, "FAILED")]
    assert "ValueError"   in cierre["notes"]
    assert "origen vacio" in cierre["notes"]


# ─────────────────────────────────────────────────────────────────────────────
# La tabla debe ser autocontenida: duración por resta, sin self-join
# ─────────────────────────────────────────────────────────────────────────────

def test_el_cierre_repite_el_started_at_de_la_apertura(ops):
    run_id = ops.log_start("ventas")
    ops.log_success(run_id, "ventas")

    filas = _filas(ops)
    assert filas[(run_id, "SUCCESS")]["started_at"] == \
           filas[(run_id, "STARTED")]["started_at"]


def test_la_duracion_sale_de_la_fila_de_cierre(ops):
    run_id = ops.log_start("ventas")
    ops.log_success(run_id, "ventas")

    cierre = _filas(ops)[(run_id, "SUCCESS")]
    assert cierre["started_at"]  is not None
    assert cierre["finished_at"] is not None
    assert (cierre["finished_at"] - cierre["started_at"]).total_seconds() >= 0


def test_cierre_sin_apertura_conocida_se_escribe_igual(ops):
    """
    Proceso reiniciado entre apertura y cierre. La fila se pierde si el esquema
    no admite `started_at` nulo — que era exactamente el bug.
    """
    ops.log_success("de-otro-proceso", "ventas", rows_written=7)

    cierre = _filas(ops)[("de-otro-proceso", "SUCCESS")]
    assert cierre["started_at"]   is None
    assert cierre["rows_written"] == 7


# ─────────────────────────────────────────────────────────────────────────────
# Esquema — aseverado sobre el StructType real, no sobre el texto del fichero
# ─────────────────────────────────────────────────────────────────────────────

def test_el_esquema_declara_los_campos_en_orden():
    from DKOps.ingestion.ops.ops_logger import _OPS_SCHEMA

    assert [f.name for f in _OPS_SCHEMA.fields] == [
        "run_id", "pipeline", "dataset", "status",
        "rows_read", "rows_written",
        "started_at", "finished_at", "notes",
    ]


def test_las_marcas_de_tiempo_admiten_nulos():
    """Causa raiz del #28."""
    from DKOps.ingestion.ops.ops_logger import _OPS_SCHEMA

    campos = {f.name: f for f in _OPS_SCHEMA.fields}
    assert campos["started_at"].nullable  is True
    assert campos["finished_at"].nullable is True


def test_los_campos_de_identidad_siguen_siendo_obligatorios():
    from DKOps.ingestion.ops.ops_logger import _OPS_SCHEMA

    campos = {f.name: f for f in _OPS_SCHEMA.fields}
    for obligatorio in ("run_id", "pipeline", "dataset", "status"):
        assert campos[obligatorio].nullable is False


# ─────────────────────────────────────────────────────────────────────────────
# Varias ejecuciones: el log agrega correctamente
# ─────────────────────────────────────────────────────────────────────────────

def test_varias_ejecuciones_se_agregan_por_estado(ops):
    for _ in range(3):
        ops.log_success(ops.log_start("ventas"), "ventas", rows_written=10)
    ops.log_failure(ops.log_start("ventas"), "ventas", RuntimeError("boom"))

    conteo = {
        r["status"]: r["n"]
        for r in ops.read().groupBy("status").count()
                    .withColumnRenamed("count", "n").collect()
    }
    assert conteo == {"STARTED": 4, "SUCCESS": 3, "FAILED": 1}
