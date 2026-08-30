"""
test_cdc_is_deleted.py — Tests del relleno de `is_deleted` en cdc_merge.

Cubre el issue #25: el default solo se aplicaba cuando la columna **faltaba**
en el DataFrame. Si llegaba desde Bronze con nulos, esos nulos se propagaban a
Silver y las filas desaparecían de cualquier consulta `WHERE NOT is_deleted`.

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
from DKOps.ingestion.strategies import cdc_merge as cdc_mod

# `cdc_merge` hace `from pyspark.sql import functions as F`, que resuelve por
# atributo sobre el mock de `pyspark.sql` — no es el objeto registrado en
# sys.modules. Hay que aseverar sobre el mismo F que usa el modulo.
F = cdc_mod.F


CON_IS_DELETED = (
    ColumnContract(name="cliente_id", type="STRING",  nullable=False),
    ColumnContract(name="nombre",     type="STRING",  nullable=True),
    ColumnContract(name="is_deleted", type="BOOLEAN", nullable=True),
)

SIN_IS_DELETED = CON_IS_DELETED[:2]


def _dst_contract(columns=CON_IS_DELETED) -> TableContract:
    return TableContract(
        catalog="ct_silver_dev", schema="cdc", name="clientes_current",
        type="MANAGED", format="DELTA", columns=columns,
    )


def _ing_contract() -> IngestionContract:
    return IngestionContract(
        name                      = "clientes_current",
        ingest_type               = IngestionType.BATCH,
        source                    = SourceSpec(format="delta"),
        destination_contract_path = "../../tables/silver/clientes_current.json",
        metadata                  = MetadataConfig(),
        checkpoint_suffix         = "clientes",
        strategy                  = SilverStrategy.CDC_MERGE,
        merge_keys                = ("cliente_id",),
    )


def _strategy(dst_contract):
    from DKOps.ingestion.strategies.cdc_merge import CdcMergeStrategy
    with patch("DKOps.ingestion.strategies.base.TableReader"), \
         patch("DKOps.ingestion.strategies.base.TableWriter"):
        return CdcMergeStrategy(
            spark=MagicMock(), contract=_ing_contract(),
            src_contract=dst_contract, dst_contract=dst_contract,
        )


def _df(columns):
    d = MagicMock()
    d.withColumn.return_value = d
    d.columns = list(columns)
    return d


def _llamada(df):
    """(nombre_columna, expresion) del ultimo withColumn."""
    c = df.withColumn.call_args
    return c.args[0], c.args[1]


# ── La columna NO viene en el DataFrame: se rellena con literal ───────────────

def test_columna_ausente_se_rellena_con_false():
    strat = _strategy(_dst_contract())
    df    = _df(["cliente_id", "nombre"])

    strat._ensure_is_deleted(df, deleted=False)

    nombre, _ = _llamada(df)
    assert nombre == "is_deleted"
    F.lit.assert_called_with(False)


# ── La columna SÍ viene: hay que coalescer, no ignorarla (issue #25) ──────────

def test_columna_presente_con_nulos_se_coalesce():
    strat = _strategy(_dst_contract())
    df    = _df(["cliente_id", "nombre", "is_deleted"])

    F.coalesce.reset_mock()

    strat._ensure_is_deleted(df, deleted=False)

    nombre, _ = _llamada(df)
    assert nombre == "is_deleted"
    assert F.coalesce.called, (
        "con la columna presente hay que rellenar los nulos, no dejarlos pasar"
    )


def test_columna_presente_no_se_ignora():
    """Regresion directa del issue #25: antes esto era un no-op."""
    strat = _strategy(_dst_contract())
    df    = _df(["cliente_id", "nombre", "is_deleted"])

    strat._ensure_is_deleted(df, deleted=False)

    assert df.withColumn.called, (
        "el DataFrame salio intacto — los nulos llegarian a Silver"
    )


# ── Eventos D: la baja se marca siempre ───────────────────────────────────────

def test_delete_marca_true_aunque_la_columna_venga_del_origen():
    strat = _strategy(_dst_contract())
    df    = _df(["cliente_id", "nombre", "is_deleted"])

    strat._ensure_is_deleted(df, deleted=True)

    nombre, _ = _llamada(df)
    assert nombre == "is_deleted"
    F.lit.assert_called_with(True)


def test_delete_marca_true_si_la_columna_no_viene():
    strat = _strategy(_dst_contract())
    df    = _df(["cliente_id", "nombre"])

    strat._ensure_is_deleted(df, deleted=True)

    F.lit.assert_called_with(True)


# ── El contrato manda: sin is_deleted declarado, no se toca nada ──────────────

def test_noop_si_el_contrato_no_declara_is_deleted():
    strat = _strategy(_dst_contract(SIN_IS_DELETED))
    df    = _df(["cliente_id", "nombre", "is_deleted"])

    assert strat._ensure_is_deleted(df, deleted=False) is df
    df.withColumn.assert_not_called()
