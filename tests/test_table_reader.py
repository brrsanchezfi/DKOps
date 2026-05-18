"""
test_table_reader.py
====================
Tests para TableReader y el campo change_data_feed en TableContract.

No requiere Spark real — todos los tests usan mocks.

Ejecutar:
    pytest tests/test_table_reader.py -v
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions",
    "pyspark.sql.types", "pyspark.sql.dataframe",
    "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.table_governance.contracts.loader import ColumnContract, TableContract
from DKOps.table_governance.readers.table_reader import TableReader


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_contract(
    change_data_feed: bool = False,
    partitions: tuple[str, ...] = (),
) -> TableContract:
    return TableContract(
        catalog  = "ct_bronze_dev",
        schema   = "ecommerce",
        name     = "pedidos",
        type     = "MANAGED",
        format   = "DELTA",
        columns  = (
            ColumnContract(name="pedido_id",   type="STRING", nullable=False),
            ColumnContract(name="fecha_pedido", type="DATE",   nullable=False),
            ColumnContract(name="total_usd",   type="DOUBLE", nullable=True),
            ColumnContract(name="estado",      type="STRING", nullable=True),
        ),
        partitions       = partitions,
        change_data_feed = change_data_feed,
    )


def _mock_launcher(is_databricks: bool = False) -> MagicMock:
    launcher              = MagicMock()
    launcher.env._is_databricks = is_databricks
    launcher.env.env            = "dev"
    launcher.spark              = MagicMock()
    return launcher


@contextmanager
def _patch_reader(is_databricks: bool = False):
    """Parchea Launcher en table_reader y devuelve el mock del launcher."""
    mock = _mock_launcher(is_databricks)
    with patch(
        "DKOps.table_governance.readers.table_reader.Launcher",
        **{"current.return_value": mock},
    ):
        yield mock


def _mock_df(columns: list[str] | None = None) -> MagicMock:
    """DataFrame mock con .columns, .filter(), .select(), .limit() encadenables."""
    df          = MagicMock()
    df.columns  = columns or ["pedido_id", "fecha_pedido", "total_usd", "estado"]
    df.filter.return_value  = df
    df.select.return_value  = df
    df.limit.return_value   = df
    df.count.return_value   = 42
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TC-CTR  change_data_feed en TableContract
# ─────────────────────────────────────────────────────────────────────────────

def test_cdf_default_false():
    ct = _make_contract()
    assert ct.change_data_feed is False


def test_cdf_true_parsed():
    ct = _make_contract(change_data_feed=True)
    assert ct.change_data_feed is True


def test_cdf_in_tblproperties_ddl():
    """_build_create_ddl debe incluir enableChangeDataFeed cuando cdf=True."""
    from DKOps.table_governance.writers.base_writer import BaseWriter

    contract = _make_contract(change_data_feed=True)
    mock     = _mock_launcher(is_databricks=True)

    with patch("DKOps.table_governance.writers.base_writer.Launcher",
               **{"current.return_value": mock}), \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator"):

        from DKOps.table_governance.writers.create_writer import CreateWriter
        writer = CreateWriter(contract)
        ddl    = writer._build_create_ddl(or_replace=False)

    assert "enableChangeDataFeed" in ddl
    assert "true" in ddl


# ─────────────────────────────────────────────────────────────────────────────
# TC-TR01  read() — lectura completa
# ─────────────────────────────────────────────────────────────────────────────

def test_read_returns_dataframe():
    contract = _make_contract()
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        result = TableReader(contract).read()

    mock.spark.read.table.assert_called_once_with("ecommerce.pedidos")
    assert result is df_mock


def test_read_applies_filter():
    contract = _make_contract()
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        TableReader(contract).read(filter="estado = 'ACTIVE'")

    df_mock.filter.assert_called_once_with("estado = 'ACTIVE'")


def test_read_applies_columns_select():
    contract = _make_contract()
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        TableReader(contract).read(columns=["pedido_id", "total_usd"])

    df_mock.select.assert_called_once_with("pedido_id", "total_usd")


def test_read_applies_limit():
    contract = _make_contract()
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        TableReader(contract).read(limit=10)

    df_mock.limit.assert_called_once_with(10)


def test_read_invalid_column_raises():
    contract = _make_contract()
    df_mock  = _mock_df(columns=["pedido_id", "estado"])

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        with pytest.raises(ValueError, match="col_inexistente"):
            TableReader(contract).read(columns=["pedido_id", "col_inexistente"])


def test_read_negative_limit_raises():
    contract = _make_contract()
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        with pytest.raises(ValueError, match="limit"):
            TableReader(contract).read(limit=-1)


# ─────────────────────────────────────────────────────────────────────────────
# TC-TR02  read_partition() — lectura por partición
# ─────────────────────────────────────────────────────────────────────────────

def test_read_partition_applies_filter():
    contract = _make_contract(partitions=("fecha_pedido",))
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        TableReader(contract).read_partition({"fecha_pedido": "2024-01-15"})

    df_mock.filter.assert_called_once_with("`fecha_pedido` = '2024-01-15'")


def test_read_partition_multi_col_filter():
    contract = _make_contract(partitions=("fecha_pedido", "estado"))
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        TableReader(contract).read_partition(
            {"fecha_pedido": "2024-01-15", "estado": "DELIVERED"}
        )

    call_args = df_mock.filter.call_args[0][0]
    assert "fecha_pedido" in call_args
    assert "estado" in call_args


def test_read_partition_empty_raises():
    contract = _make_contract(partitions=("fecha_pedido",))

    with _patch_reader():
        with pytest.raises(ValueError, match="vacío"):
            TableReader(contract).read_partition({})


def test_read_partition_non_partition_col_raises():
    contract = _make_contract(partitions=("fecha_pedido",))
    df_mock  = _mock_df()

    with _patch_reader() as mock:
        mock.spark.read.table.return_value = df_mock
        with pytest.raises(ValueError, match="total_usd"):
            TableReader(contract).read_partition({"total_usd": "100"})


# ─────────────────────────────────────────────────────────────────────────────
# TC-TR03  read_stream() — streaming DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def test_read_stream_uses_readstream():
    contract    = _make_contract()
    stream_mock = MagicMock()

    with _patch_reader() as mock:
        mock.spark.readStream.format.return_value.table.return_value = stream_mock
        result = TableReader(contract).read_stream()

    mock.spark.readStream.format.assert_called_once_with("delta")
    mock.spark.readStream.format.return_value.table.assert_called_once_with(
        "ecommerce.pedidos"
    )
    assert result is stream_mock


# ─────────────────────────────────────────────────────────────────────────────
# TC-TR04  read_cdf() — Change Data Feed
# ─────────────────────────────────────────────────────────────────────────────

def test_read_cdf_raises_if_cdf_disabled():
    contract = _make_contract(change_data_feed=False)

    with _patch_reader():
        with pytest.raises(ValueError, match="change_data_feed"):
            TableReader(contract).read_cdf(starting_version=0)


def test_read_cdf_raises_if_no_starting_point():
    contract = _make_contract(change_data_feed=True)

    with _patch_reader():
        with pytest.raises(ValueError, match="starting_version"):
            TableReader(contract).read_cdf()


def test_read_cdf_raises_if_both_starting_params():
    contract = _make_contract(change_data_feed=True)

    with _patch_reader():
        with pytest.raises(ValueError, match="solo uno"):
            TableReader(contract).read_cdf(
                starting_version=1,
                starting_timestamp="2024-01-01",
            )


def test_read_cdf_by_version():
    contract = _make_contract(change_data_feed=True)
    df_mock  = MagicMock()

    with _patch_reader() as mock:
        reader_chain = MagicMock()
        reader_chain.option.return_value = reader_chain
        reader_chain.table.return_value  = df_mock
        mock.spark.read.format.return_value = reader_chain

        result = TableReader(contract).read_cdf(starting_version=3)

    mock.spark.read.format.assert_called_once_with("delta")
    option_calls = [str(c) for c in reader_chain.option.call_args_list]
    assert any("readChangeFeed" in c and "true" in c for c in option_calls)
    assert any("startingVersion" in c and "3" in c for c in option_calls)
    assert result is df_mock


def test_read_cdf_by_timestamp():
    contract = _make_contract(change_data_feed=True)
    df_mock  = MagicMock()

    with _patch_reader() as mock:
        reader_chain = MagicMock()
        reader_chain.option.return_value = reader_chain
        reader_chain.table.return_value  = df_mock
        mock.spark.read.format.return_value = reader_chain

        TableReader(contract).read_cdf(starting_timestamp="2024-01-01T00:00:00")

    option_calls = [str(c) for c in reader_chain.option.call_args_list]
    assert any("startingTimestamp" in c for c in option_calls)
    assert not any("startingVersion" in c for c in option_calls)


def test_read_cdf_with_ending_version():
    contract = _make_contract(change_data_feed=True)

    with _patch_reader() as mock:
        reader_chain = MagicMock()
        reader_chain.option.return_value = reader_chain
        reader_chain.table.return_value  = MagicMock()
        mock.spark.read.format.return_value = reader_chain

        TableReader(contract).read_cdf(starting_version=2, ending_version=5)

    option_calls = [str(c) for c in reader_chain.option.call_args_list]
    assert any("endingVersion" in c and "5" in c for c in option_calls)


# ─────────────────────────────────────────────────────────────────────────────
# TC-TR05  repr y nombre efectivo
# ─────────────────────────────────────────────────────────────────────────────

def test_reader_repr():
    contract = _make_contract()
    with _patch_reader():
        r = repr(TableReader(contract))
    assert "ct_bronze_dev.ecommerce.pedidos" in r


def test_reader_uses_full_name_on_databricks():
    contract = _make_contract()

    with _patch_reader(is_databricks=True) as mock:
        mock.spark.read.table.return_value = _mock_df()
        TableReader(contract).read()

    mock.spark.read.table.assert_called_once_with("ct_bronze_dev.ecommerce.pedidos")


def test_reader_uses_schema_name_on_local():
    contract = _make_contract()

    with _patch_reader(is_databricks=False) as mock:
        mock.spark.read.table.return_value = _mock_df()
        TableReader(contract).read()

    mock.spark.read.table.assert_called_once_with("ecommerce.pedidos")
