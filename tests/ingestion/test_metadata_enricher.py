"""
test_metadata_enricher.py — Tests del enriquecedor de metadata.

Verifica que MetadataEnricher llama las funciones correctas de Spark
y añade las columnas según la config. Usa mocks de pyspark.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.dataframe", "pyspark.sql.window", "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.ingestion.contracts.ingestion_contract import MetadataConfig
from DKOps.ingestion.enrichment.metadata import MetadataEnricher


def _mock_df():
    """DataFrame mock con withColumn encadenado."""
    df = MagicMock()
    df.withColumn.return_value = df  # encadenamiento fluido
    return df


class TestMetadataEnricher:

    @pytest.fixture
    def enricher(self) -> MetadataEnricher:
        return MetadataEnricher()

    def test_adds_ingested_at_column(self, enricher):
        config = MetadataConfig(add_ingested_at=True, add_ingested_date=False, add_source_file=False)
        df = _mock_df()
        enricher.enrich(df, config)
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        assert "_ingested_at" in calls

    def test_adds_ingested_date_column(self, enricher):
        config = MetadataConfig(add_ingested_at=False, add_ingested_date=True, add_source_file=False)
        df = _mock_df()
        enricher.enrich(df, config)
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        assert "_ingested_date" in calls

    def test_adds_source_file_for_non_kafka(self, enricher):
        config = MetadataConfig(add_ingested_at=False, add_ingested_date=False, add_source_file=True)
        df = _mock_df()
        enricher.enrich(df, config, source_format="json")
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        assert "_source_file" in calls

    def test_skips_source_file_for_kafka(self, enricher):
        config = MetadataConfig(add_ingested_at=False, add_ingested_date=False, add_source_file=True)
        df = _mock_df()
        enricher.enrich(df, config, source_format="kafka")
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        assert "_source_file" not in calls

    def test_all_standard_columns_added(self, enricher):
        config = MetadataConfig(
            add_ingested_at=True, add_ingested_date=True, add_source_file=True,
        )
        df = _mock_df()
        enricher.enrich(df, config, source_format="parquet")
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        for col in ["_ingested_at", "_ingested_date", "_source_file"]:
            assert col in calls, f"Columna {col} no encontrada en withColumn calls"

    def test_silver_timestamps_added(self, enricher):
        config = MetadataConfig(
            add_ingested_at=False, add_ingested_date=False,
            add_source_file=False, add_silver_timestamps=True,
        )
        df = _mock_df()
        enricher.enrich(df, config)
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        assert "_silver_created_at"  in calls
        assert "_silver_modified_at" in calls

    def test_no_calls_when_all_false(self, enricher):
        config = MetadataConfig(
            add_ingested_at=False, add_ingested_date=False,
            add_source_file=False, add_kafka_metadata=False,
            add_silver_timestamps=False,
        )
        df = _mock_df()
        enricher.enrich(df, config)
        df.withColumn.assert_not_called()

    def test_kafka_metadata_columns_added(self, enricher):
        config = MetadataConfig(
            add_ingested_at=False, add_ingested_date=False,
            add_source_file=False, add_kafka_metadata=True,
        )
        df = _mock_df()
        enricher.enrich(df, config, source_format="kafka")
        calls = [c[0][0] for c in df.withColumn.call_args_list]
        for col in ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_ts", "_raw_value"]:
            assert col in calls, f"{col} debería añadirse con add_kafka_metadata=True"

    def test_returns_enriched_df(self, enricher):
        """enrich() devuelve el DataFrame (posiblemente transformado)."""
        config = MetadataConfig()
        df     = _mock_df()
        result = enricher.enrich(df, config)
        # withColumn encadenado devuelve el mismo mock
        assert result is not None
