"""
test_reader_factory.py — Tests de la fábrica de readers.

Prueba que SourceReaderFactory selecciona el reader correcto
en función del entorno (local vs Databricks) y el tipo de fuente.
No instancia Spark — los readers se comprueban por tipo.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Mock pyspark antes de importar DKOps
from unittest.mock import MagicMock
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.dataframe", "pyspark.sql.window", "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract, IngestionType, LoadType, MetadataConfig,
    SourceSpec, StreamTrigger,
)
from DKOps.ingestion.readers.factory import SourceReaderFactory
from DKOps.ingestion.readers.local_batch import LocalBatchReader
from DKOps.ingestion.readers.file_stream import FileStreamReader


def _make_contract(
    ingest_type: str = "batch",
    fmt:         str = "json",
    path:        str = "/tmp/data",
) -> IngestionContract:
    return IngestionContract(
        name                      = "test",
        ingest_type               = IngestionType(ingest_type),
        load_type                 = LoadType.INCREMENTAL,
        source                    = SourceSpec(format=fmt, path=path),
        destination_contract_path = "tables/bronze/test.json",
        metadata                  = MetadataConfig(),
        checkpoint_suffix         = "bronze/test",
    )


class TestSourceReaderFactory:

    def test_local_batch_returns_local_batch_reader(self, local_env_config):
        spark    = MagicMock()
        contract = _make_contract("batch", "json")
        reader   = SourceReaderFactory.create(contract, spark, local_env_config)
        assert isinstance(reader, LocalBatchReader)

    def test_local_streaming_returns_file_stream_reader(self, local_env_config):
        spark    = MagicMock()
        contract = _make_contract("streaming", "json")
        reader   = SourceReaderFactory.create(contract, spark, local_env_config)
        assert isinstance(reader, FileStreamReader)

    def test_databricks_batch_returns_autoloader(self, databricks_env_config):
        spark    = MagicMock()
        contract = _make_contract("batch", "parquet")

        # factory usa import local, se parchea en el módulo de origen
        with patch("DKOps.ingestion.readers.autoloader.AutoLoaderReader") as MockAL:
            MockAL.return_value = MagicMock()
            reader = SourceReaderFactory.create(
                contract, spark, databricks_env_config, schema_root="/tmp/schemas"
            )
            MockAL.assert_called_once()

    def test_databricks_streaming_returns_autoloader(self, databricks_env_config):
        spark    = MagicMock()
        contract = _make_contract("streaming", "json")

        with patch("DKOps.ingestion.readers.autoloader.AutoLoaderReader") as MockAL:
            MockAL.return_value = MagicMock()
            SourceReaderFactory.create(
                contract, spark, databricks_env_config, schema_root="/tmp/schemas"
            )
            MockAL.assert_called_once()

    def test_kafka_always_returns_kafka_reader(self, local_env_config):
        spark    = MagicMock()
        contract = _make_contract("streaming", "kafka")

        with patch("DKOps.ingestion.readers.kafka.KafkaReader") as MockKR:
            MockKR.return_value = MagicMock()
            SourceReaderFactory.create(contract, spark, local_env_config)
            MockKR.assert_called_once()

    def test_kafka_in_databricks_also_uses_kafka_reader(self, databricks_env_config):
        spark    = MagicMock()
        contract = _make_contract("streaming", "kafka")

        with patch("DKOps.ingestion.readers.kafka.KafkaReader") as MockKR:
            MockKR.return_value = MagicMock()
            SourceReaderFactory.create(contract, spark, databricks_env_config)
            MockKR.assert_called_once()

    def test_delta_source_uses_local_reader_even_in_databricks(self, databricks_env_config):
        """Delta sources (Silver reads from Bronze) no pasan por Auto Loader."""
        spark    = MagicMock()
        contract = _make_contract("batch", "delta")
        reader   = SourceReaderFactory.create(contract, spark, databricks_env_config)
        assert isinstance(reader, LocalBatchReader)
