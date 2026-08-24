"""
test_silver_timestamps.py — Tests de las columnas técnicas de Silver.

Cubre el issue #19: `add_silver_timestamps` debe producir las mismas dos
columnas (`_silver_created_at` y `_silver_modified_at`) tanto en el camino
de MetadataEnricher (Bronze) como en el de las estrategias de promoción
(Silver), y `_silver_created_at` no debe sobrescribirse en cada MERGE.

Usa mocks de pyspark — no requiere Spark real.
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

for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.dataframe", "pyspark.sql.window", "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract, IngestionType, MetadataConfig, SilverStrategy, SourceSpec,
)
from DKOps.table_governance.contracts.loader import ColumnContract, TableContract


SILVER_COLS = (
    ColumnContract(name="venta_id",            type="STRING",    nullable=False),
    ColumnContract(name="importe",             type="DOUBLE",    nullable=True),
    ColumnContract(name="_silver_created_at",  type="TIMESTAMP", nullable=True),
    ColumnContract(name="_silver_modified_at", type="TIMESTAMP", nullable=True),
)


def _silver_contract(columns=SILVER_COLS) -> TableContract:
    return TableContract(
        catalog = "ct_silver_dev",
        schema  = "batch",
        name    = "ventas",
        type    = "MANAGED",
        format  = "DELTA",
        columns = columns,
    )


def _ingestion_contract(add_silver_timestamps: bool = True) -> IngestionContract:
    return IngestionContract(
        name                      = "ventas_current",
        ingest_type               = IngestionType.BATCH,
        source                    = SourceSpec(format="delta"),
        destination_contract_path = "../../tables/silver/ventas.json",
        metadata                  = MetadataConfig(
            add_silver_timestamps = add_silver_timestamps,
        ),
        checkpoint_suffix         = "ventas",
        strategy                  = SilverStrategy.FULL_MERGE,
        merge_keys                = ("venta_id",),
    )


def _make_strategy(ing_contract, dst_contract):
    """FullMergeStrategy con TableReader/TableWriter mockeados."""
    from DKOps.ingestion.strategies.full_merge import FullMergeStrategy

    with patch("DKOps.ingestion.strategies.base.TableReader"), \
         patch("DKOps.ingestion.strategies.base.TableWriter"):
        return FullMergeStrategy(
            spark        = MagicMock(),
            contract     = ing_contract,
            src_contract = dst_contract,
            dst_contract = dst_contract,
        )


def _mock_df(columns: list[str] | None = None):
    df = MagicMock()
    df.withColumn.return_value = df
    df.columns = columns or []
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TC-ST01  _add_silver_timestamps añade AMBAS columnas
# ─────────────────────────────────────────────────────────────────────────────

def test_add_silver_timestamps_anade_ambas_columnas():
    strat = _make_strategy(_ingestion_contract(True), _silver_contract())
    df    = _mock_df()

    strat._add_silver_timestamps(df)

    añadidas = [c.args[0] for c in df.withColumn.call_args_list]
    assert "_silver_created_at"  in añadidas
    assert "_silver_modified_at" in añadidas


def test_add_silver_timestamps_noop_si_el_flag_esta_desactivado():
    strat = _make_strategy(_ingestion_contract(False), _silver_contract())
    df    = _mock_df()

    assert strat._add_silver_timestamps(df) is df
    df.withColumn.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TC-ST02  Paridad con MetadataEnricher — mismas columnas por ambos caminos
# ─────────────────────────────────────────────────────────────────────────────

def test_paridad_con_metadata_enricher():
    from DKOps.ingestion.enrichment.metadata import MetadataEnricher

    df_enricher = _mock_df()
    MetadataEnricher().enrich(
        df_enricher,
        MetadataConfig(
            add_ingested_at   = False,
            add_ingested_date = False,
            add_source_file   = False,
            add_silver_timestamps = True,
        ),
    )
    cols_enricher = {c.args[0] for c in df_enricher.withColumn.call_args_list}

    strat      = _make_strategy(_ingestion_contract(True), _silver_contract())
    df_strat   = _mock_df()
    strat._add_silver_timestamps(df_strat)
    cols_strat = {c.args[0] for c in df_strat.withColumn.call_args_list}

    assert cols_enricher == cols_strat == {"_silver_created_at", "_silver_modified_at"}


# ─────────────────────────────────────────────────────────────────────────────
# TC-ST03  _silver_insert_only_columns protege _silver_created_at en el MERGE
# ─────────────────────────────────────────────────────────────────────────────

def test_insert_only_columns_incluye_created_at():
    strat = _make_strategy(_ingestion_contract(True), _silver_contract())
    assert strat._silver_insert_only_columns() == ["_silver_created_at"]


def test_insert_only_columns_vacio_si_el_flag_esta_desactivado():
    strat = _make_strategy(_ingestion_contract(False), _silver_contract())
    assert strat._silver_insert_only_columns() == []


def test_insert_only_columns_vacio_si_el_contrato_no_declara_created_at():
    """Contrato Silver legacy que solo declara _silver_modified_at."""
    cols = tuple(c for c in SILVER_COLS if c.name != "_silver_created_at")
    strat = _make_strategy(_ingestion_contract(True), _silver_contract(cols))
    assert strat._silver_insert_only_columns() == []


# ─────────────────────────────────────────────────────────────────────────────
# TC-ST04  Retrocompatibilidad — _select_for_silver descarta la columna extra
#          si el contrato no la declara
# ─────────────────────────────────────────────────────────────────────────────

def test_select_for_silver_descarta_created_at_no_declarado():
    cols  = tuple(c for c in SILVER_COLS if c.name != "_silver_created_at")
    strat = _make_strategy(_ingestion_contract(True), _silver_contract(cols))

    df = _mock_df([
        "venta_id", "importe", "_silver_created_at", "_silver_modified_at",
    ])
    strat._select_for_silver(df)

    seleccionadas = list(df.select.call_args.args)
    assert "_silver_created_at" not in seleccionadas
    assert "_silver_modified_at" in seleccionadas
