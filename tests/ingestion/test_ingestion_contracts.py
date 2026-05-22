"""
test_ingestion_contracts.py — Tests del sistema de contratos de ingesta.

Prueba sin necesidad de Spark:
  - Parsing de JSON a IngestionContract
  - Resolución de placeholders {path.*}, {catalog.*}
  - Validación de campos obligatorios
  - Enums IngestionType, LoadType, SilverStrategy
  - IngestionContractLoader carga y filtra enabled=false
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC  = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Mock pyspark antes de importar DKOps — igual que los tests existentes.
# Permite correr los tests de contratos sin PySpark instalado.
from unittest.mock import MagicMock
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.dataframe", "pyspark.sql.window", "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract,
    IngestionType,
    LoadType,
    MetadataConfig,
    SilverStrategy,
    SourceSpec,
    StreamTrigger,
)
from DKOps.ingestion.contracts.loader import IngestionContractLoader


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_batch_json(name: str = "test_dataset", enabled: bool = True) -> dict:
    return {
        "name":        name,
        "ingest_type": "batch",
        "load_type":   "incremental",
        "enabled":     enabled,
        "source": {
            "format": "json",
            "path":   "/tmp/landing/test",
        },
        "destination_contract": "../../tables/bronze/test_raw.json",
        "metadata": {
            "add_ingested_at":   True,
            "add_ingested_date": True,
            "add_source_file":   True,
        },
    }


def _make_silver_json(name: str = "test_silver") -> dict:
    return {
        "name":               name,
        "ingest_type":        "batch",
        "strategy":           "full_merge",
        "source":             {"format": "delta"},
        "source_contract":    "../../tables/bronze/test_raw.json",
        "destination_contract": "../../tables/silver/test_current.json",
        "merge_keys":         ["id"],
        "watermark_col":      "fecha",
        "metadata":           {"add_silver_timestamps": True},
    }


def _write_json(tmp_path: Path, filename: str, data: dict) -> Path:
    path = tmp_path / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _make_loader(tmp_path: Path, env) -> IngestionContractLoader:
    return IngestionContractLoader(
        contracts_dir = tmp_path,
        base_dir      = tmp_path,
        env           = env,
    )


# ── Tests de parsing de contratos ────────────────────────────────────────────

class TestIngestionContractParsing:

    def test_batch_contract_parse(self, tmp_path, local_env_config):
        path   = _write_json(tmp_path, "test.json", _make_batch_json())
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)

        assert c.name        == "test_dataset"
        assert c.ingest_type == IngestionType.BATCH
        assert c.load_type   == LoadType.INCREMENTAL
        assert c.enabled     is True
        assert c.source.format == "json"
        assert c.metadata.add_ingested_at   is True
        assert c.metadata.add_source_file   is True

    def test_streaming_contract_parse(self, tmp_path, local_env_config):
        data = {
            "name":        "stream_test",
            "ingest_type": "streaming",
            "trigger":     "available_now",
            "source":      {"format": "json", "path": "/tmp/stream"},
            "destination_contract": "../../tables/bronze/stream_raw.json",
        }
        path   = _write_json(tmp_path, "stream.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)

        assert c.ingest_type == IngestionType.STREAMING
        assert c.trigger     == StreamTrigger.AVAILABLE_NOW
        assert c.is_streaming() is True
        assert c.is_batch()     is False

    def test_silver_contract_parse(self, tmp_path, local_env_config):
        path   = _write_json(tmp_path, "silver.json", _make_silver_json())
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)

        assert c.strategy            == SilverStrategy.FULL_MERGE
        assert c.merge_keys          == ("id",)
        assert c.watermark_col       == "fecha"
        assert c.is_silver_promotion() is True
        assert c.source_contract_path is not None

    def test_missing_name_raises(self, tmp_path, local_env_config):
        data = {"ingest_type": "batch", "source": {"format": "json"}, "destination_contract": "x"}
        path = _write_json(tmp_path, "bad.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        with pytest.raises(ValueError, match="'name' obligatorio"):
            loader.load(path)

    def test_missing_destination_contract_raises(self, tmp_path, local_env_config):
        data = {"name": "test", "ingest_type": "batch", "source": {"format": "json"}}
        path = _write_json(tmp_path, "bad.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        with pytest.raises(ValueError, match="'destination_contract' obligatorio"):
            loader.load(path)

    def test_invalid_ingest_type_raises(self, tmp_path, local_env_config):
        data = {
            "name": "test", "ingest_type": "unknown",
            "source": {"format": "json"},
            "destination_contract": "x.json",
        }
        path = _write_json(tmp_path, "bad.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        with pytest.raises(ValueError):
            loader.load(path)


# ── Tests de resolución de placeholders ──────────────────────────────────────

class TestPlaceholderResolution:

    def test_path_landing_resolved(self, tmp_path, local_env_config):
        data = _make_batch_json()
        data["source"]["path"] = "{path.landing}/ventas"
        path   = _write_json(tmp_path, "test.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)
        assert "/tmp/dkops_test/landing/ventas" == c.source.path

    def test_unknown_placeholder_raises(self, tmp_path, local_env_config):
        data = _make_batch_json()
        data["source"]["path"] = "{path.nonexistent}/data"
        path   = _write_json(tmp_path, "test.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        with pytest.raises(KeyError, match="nonexistent"):
            loader.load(path)

    def test_env_placeholder_resolved(self, tmp_path, local_env_config):
        data = _make_batch_json()
        data["description"] = "Entorno: {env} ({env_short})"
        path   = _write_json(tmp_path, "test.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)
        assert "local" in c.description
        assert "(l)"   in c.description


# ── Tests de load_all ─────────────────────────────────────────────────────────

class TestLoadAll:

    def test_load_all_skips_disabled(self, tmp_path, local_env_config):
        _write_json(tmp_path, "a.json", _make_batch_json("a", enabled=True))
        _write_json(tmp_path, "b.json", _make_batch_json("b", enabled=False))
        loader    = _make_loader(tmp_path, local_env_config)
        contracts = loader.load_all()
        names     = [c.name for c in contracts]
        assert "a" in names
        assert "b" not in names

    def test_load_all_returns_all_enabled(self, tmp_path, local_env_config):
        for i in range(3):
            _write_json(tmp_path, f"ds{i}.json", _make_batch_json(f"ds{i}"))
        loader    = _make_loader(tmp_path, local_env_config)
        contracts = loader.load_all()
        assert len(contracts) == 3


# ── Tests de MetadataConfig defaults ─────────────────────────────────────────

class TestMetadataConfig:

    def test_defaults_are_sensible(self, tmp_path, local_env_config):
        data = _make_batch_json()
        data.pop("metadata")  # sin sección metadata → defaults
        path   = _write_json(tmp_path, "test.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)

        assert c.metadata.add_ingested_at    is True
        assert c.metadata.add_ingested_date  is True
        assert c.metadata.add_source_file    is True
        assert c.metadata.add_kafka_metadata is False

    def test_kafka_metadata_opt_in(self, tmp_path, local_env_config):
        data = _make_batch_json()
        data["metadata"]["add_kafka_metadata"] = True
        path   = _write_json(tmp_path, "test.json", data)
        loader = _make_loader(tmp_path, local_env_config)
        c      = loader.load(path)
        assert c.metadata.add_kafka_metadata is True
