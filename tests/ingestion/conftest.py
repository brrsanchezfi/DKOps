"""
conftest.py — Fixtures compartidos para los tests del módulo ingestion.

Todos los tests del módulo ingestion pueden usar PySpark real a través
del fixture `spark`. Los tests que no necesitan Spark usan mocks puros.

La estrategia es igual a los tests existentes: sys.modules mock para
evitar importar pyspark en tests que no lo necesiten, y una SparkSession
real solo donde se prueba lógica de transformación/escritura.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# ── Fixtures de Spark ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """SparkSession local para tests de integración del módulo ingestion."""
    from pyspark.sql import SparkSession
    sess = (
        SparkSession.builder
            .appName("DKOps-Ingestion-Tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", "/tmp/dkops_test/warehouse")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0",
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
    )
    sess.sparkContext.setLogLevel("WARN")
    yield sess
    sess.stop()


# ── Fixtures de EnvironmentConfig ─────────────────────────────────────────────

@pytest.fixture
def local_env_config():
    """EnvironmentConfig apuntando a entorno local de test."""
    from DKOps.environment_config import EnvironmentConfig

    config = {
        "DATABRICKS_TARGET": "local",
        "environments": {
            "local": {
                "env":       "local",
                "env_short": "l",
                "catalogs":  {"bronze": "bronze", "silver": "silver", "ops": "ops"},
                "paths": {
                    "landing":    "/tmp/dkops_test/landing",
                    "bronze":     "/tmp/dkops_test/warehouse/bronze",
                    "silver":     "/tmp/dkops_test/warehouse/silver",
                    "checkpoint": "/tmp/dkops_test/checkpoints",
                },
            }
        },
    }
    return EnvironmentConfig(config, is_databricks=False)


@pytest.fixture
def databricks_env_config():
    """EnvironmentConfig simulando entorno Databricks."""
    from DKOps.environment_config import EnvironmentConfig

    config = {
        "DATABRICKS_TARGET": "dev",
        "environments": {
            "ws-123": {
                "env":       "dev",
                "env_short": "d",
                "catalogs":  {"bronze": "bronze", "silver": "silver"},
                "paths": {
                    "landing":    "abfss://landing@storage.dfs.core.windows.net",
                    "checkpoint": "abfss://chk@storage.dfs.core.windows.net",
                },
            }
        },
    }
    return EnvironmentConfig(config, is_databricks=True)
