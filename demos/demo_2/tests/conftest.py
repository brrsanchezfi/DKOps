"""
conftest.py
===========
Fixtures globales para los tests de pytest.

La fixture `spark` tiene scope=session — una sola SparkSession por
ejecución completa de tests. Esto importa porque crear SparkSession
es la operación más cara (segundos por arranque) — compartirla entre
tests reduce el tiempo total de ~30s a ~5s.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    SparkSession ligera sin Delta — los tests sobre transformaciones
    no necesitan Delta porque trabajan con DataFrames en memoria.

    Configuraciones clave para tests:
      · shuffle.partitions=2  → menos overhead en agregaciones pequeñas
      · master=local[2]       → 2 cores, suficiente para datos de test
      · UI deshabilitado      → no abre puerto 4040
    """
    spark = (
        SparkSession.builder
        .appName("dkops-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers reutilizables en múltiples tests
# ─────────────────────────────────────────────────────────────────────────────

def assert_df_equal(df_actual, df_expected, ordered: bool = False) -> None:
    """
    Compara dos DataFrames por contenido. Por defecto ignora orden de filas.

    Útil para tests de transformaciones donde el orden de salida no está garantizado
    (un groupBy no preserva orden a menos que se haga orderBy explícito).
    """
    actual_rows   = [r.asDict() for r in df_actual.collect()]
    expected_rows = [r.asDict() for r in df_expected.collect()]

    if not ordered:
        actual_rows   = sorted(actual_rows,   key=lambda r: tuple(str(v) for v in r.values()))
        expected_rows = sorted(expected_rows, key=lambda r: tuple(str(v) for v in r.values()))

    assert actual_rows == expected_rows, (
        f"\n  Esperado ({len(expected_rows)} filas): {expected_rows}\n"
        f"  Actual   ({len(actual_rows)} filas): {actual_rows}"
    )
