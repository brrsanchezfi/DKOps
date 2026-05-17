"""
test_table_writer.py
====================
Tests para TableWriter (fachada) y el comportamiento de merge_schema / mask
en los writers individuales.

Todos los tests usan mocks — no se requiere Spark real.

Ejecutar:
    pytest tests/test_table_writer.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Mock de pyspark antes de cualquier import de DKOps — los tests de writers
# no necesitan Spark real, operan sobre mocks.
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions",
    "pyspark.sql.types", "pyspark.sql.dataframe",
    "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.table_governance.contracts.loader import ColumnContract, TableContract
from DKOps.table_governance.writers.table_writer import TableWriter


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_contract(merge_schema: bool = False, masked_cols: bool = False) -> TableContract:
    if masked_cols:
        cols = (
            ColumnContract(name="id",    type="STRING",  nullable=False),
            ColumnContract(name="email", type="STRING",  nullable=True, mask="security.mask_email"),
            ColumnContract(name="valor", type="INTEGER", nullable=True),
        )
    else:
        cols = (
            ColumnContract(name="id",    type="STRING",  nullable=False),
            ColumnContract(name="valor", type="INTEGER", nullable=True),
        )
    return TableContract(
        catalog      = "ct_bronze_dev",
        schema       = "test_schema",
        name         = "test_table",
        type         = "MANAGED",
        format       = "DELTA",
        columns      = cols,
        merge_schema = merge_schema,
    )


def _mock_launcher(is_databricks: bool = False):
    """Devuelve un Launcher mockeado listo para usar en patches."""
    launcher      = MagicMock()
    launcher.env  = MagicMock()
    launcher.env._is_databricks = is_databricks
    launcher.env.env            = "dev"
    launcher.spark              = MagicMock()
    return launcher


from contextlib import contextmanager

@contextmanager
def _patch_writers(is_databricks: bool = False):
    """
    Parchea Launcher y SchemaValidator para todos los writers que heredan
    de BaseWriter. SchemaValidator se mockea para que validate() no falle
    al recibir un DataFrame mock.
    """
    mock_val_result = MagicMock()
    mock_val_result.warnings = []
    mock_val_result.raise_if_critical = MagicMock()
    mock_validator = MagicMock()
    mock_validator.validate.return_value = mock_val_result

    with patch(
        "DKOps.table_governance.writers.base_writer.Launcher",
        **{"current.return_value": _mock_launcher(is_databricks)},
    ), patch(
        "DKOps.table_governance.writers.base_writer.SchemaValidator",
        return_value=mock_validator,
    ):
        yield


def _patch_delete_launcher(is_databricks: bool = False):
    """Patch del Launcher en delete_writer (no hereda de BaseWriter)."""
    return patch(
        "DKOps.table_governance.writers.delete_writer.Launcher",
        **{"current.return_value": _mock_launcher(is_databricks)},
    )


# ─────────────────────────────────────────────────────────────────────────────
# TC-TW01  TableWriter.overwrite() — delega y soporta dry_run
# ─────────────────────────────────────────────────────────────────────────────

def test_table_writer_overwrite_dry_run():
    contract = _make_contract()
    df       = MagicMock()

    with _patch_writers():
        TableWriter(contract, dry_run=True).overwrite(df)


def test_table_writer_append_dry_run():
    contract = _make_contract()
    df       = MagicMock()

    with _patch_writers():
        TableWriter(contract, dry_run=True).append(df)


def test_table_writer_upsert_dry_run():
    contract = _make_contract()
    df       = MagicMock()

    with _patch_writers():
        TableWriter(contract, dry_run=True).upsert(df, keys=["id"])


def test_table_writer_overwrite_partition_dry_run():
    contract = _make_contract()
    # Necesita columnas de partición para poder validar
    from DKOps.table_governance.contracts.loader import TableContract as TC
    ct_partitioned = TC(
        catalog    = "ct",
        schema     = "s",
        name       = "t",
        type       = "MANAGED",
        format     = "DELTA",
        columns    = (ColumnContract(name="fecha", type="DATE", nullable=False),
                      ColumnContract(name="valor", type="INTEGER")),
        partitions = ("fecha",),
    )
    df = MagicMock()

    with _patch_writers():
        TableWriter(ct_partitioned, dry_run=True).overwrite_partition(
            df, partition={"fecha": "2024-01-01"}
        )


def test_table_writer_delete_dry_run():
    contract = _make_contract()

    with _patch_delete_launcher():
        result = TableWriter(contract, dry_run=True).delete("id = '999'")
    assert result == 0


# ─────────────────────────────────────────────────────────────────────────────
# TC-TW02  upsert sin keys lanza ValueError
# ─────────────────────────────────────────────────────────────────────────────

def test_table_writer_upsert_requires_keys():
    contract = _make_contract()
    df       = MagicMock()

    with _patch_writers():
        with pytest.raises(ValueError, match="merge_keys"):
            TableWriter(contract, dry_run=True).upsert(df, keys=[])


# ─────────────────────────────────────────────────────────────────────────────
# TC-TW03  dry_run se almacena en _writer_kwargs
# ─────────────────────────────────────────────────────────────────────────────

def test_dry_run_stored_in_writer_kwargs():
    contract = _make_contract()
    writer   = TableWriter(contract, dry_run=True)
    assert writer._dry_run is True
    assert writer._writer_kwargs["dry_run"] is True


def test_dry_run_false_by_default():
    contract = _make_contract()
    writer   = TableWriter(contract)
    assert writer._dry_run is False
    assert writer._writer_kwargs["dry_run"] is False


# ─────────────────────────────────────────────────────────────────────────────
# TC-TW04  __repr__ incluye nombre completo y dry_run
# ─────────────────────────────────────────────────────────────────────────────

def test_table_writer_repr_contains_full_name():
    contract = _make_contract()
    r        = repr(TableWriter(contract, dry_run=True))
    assert "ct_bronze_dev.test_schema.test_table" in r
    assert "dry_run=True" in r


# ─────────────────────────────────────────────────────────────────────────────
# TC-MS01  mergeSchema se activa cuando contract.merge_schema=True
# ─────────────────────────────────────────────────────────────────────────────

def test_write_df_sets_merge_schema_option():
    """
    _write_df debe llamar .option("mergeSchema","true") cuando
    contract.merge_schema=True y no se pide overwrite_schema.
    """
    from DKOps.table_governance.writers.base_writer import BaseWriter

    contract      = _make_contract(merge_schema=True)
    mock_launcher = _mock_launcher(is_databricks=True)

    # Armar la cadena de llamadas del DataFrameWriter
    mock_df     = MagicMock()
    mock_writer = MagicMock()
    mock_df.write                      = mock_writer
    mock_writer.format.return_value    = mock_writer
    mock_writer.mode.return_value      = mock_writer
    mock_writer.option.return_value    = mock_writer
    mock_writer.partitionBy.return_value = mock_writer
    mock_df.count.return_value         = 3

    mock_validator = MagicMock()
    mock_validator.validate.return_value = MagicMock(
        warnings=[], raise_if_critical=MagicMock()
    )

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator") as mock_sv:

        mock_lc.current.return_value = mock_launcher
        mock_sv.return_value         = mock_validator

        # Instanciamos AppendWriter directamente para probar _write_df
        from DKOps.table_governance.writers.append_writer import AppendWriter
        aw = AppendWriter(contract)
        aw._validator = mock_validator
        aw._write_df(mock_df, mode="append")

    option_calls = [str(c) for c in mock_writer.option.call_args_list]
    assert any("mergeSchema" in c and "true" in c for c in option_calls), (
        f"Se esperaba .option('mergeSchema','true'). Calls: {option_calls}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TC-MS02  mergeSchema NO se activa cuando merge_schema=False
# ─────────────────────────────────────────────────────────────────────────────

def test_write_df_no_merge_schema_when_disabled():
    from DKOps.table_governance.writers.append_writer import AppendWriter

    contract      = _make_contract(merge_schema=False)
    mock_launcher = _mock_launcher(is_databricks=True)

    mock_df     = MagicMock()
    mock_writer = MagicMock()
    mock_df.write                   = mock_writer
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value   = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_df.count.return_value      = 2

    mock_validator = MagicMock()
    mock_validator.validate.return_value = MagicMock(
        warnings=[], raise_if_critical=MagicMock()
    )

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator") as mock_sv:

        mock_lc.current.return_value = mock_launcher
        mock_sv.return_value         = mock_validator

        aw = AppendWriter(contract)
        aw._validator = mock_validator
        aw._write_df(mock_df, mode="append")

    option_calls = [str(c) for c in mock_writer.option.call_args_list]
    merge_calls  = [c for c in option_calls if "mergeSchema" in c]
    assert len(merge_calls) == 0, (
        f"No debería haberse llamado mergeSchema. Calls: {option_calls}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TC-MK01  _apply_column_masks se omite en local PC
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_column_masks_skipped_on_local_pc():
    from DKOps.table_governance.writers.create_writer import CreateWriter

    contract      = _make_contract(masked_cols=True)
    mock_launcher = _mock_launcher(is_databricks=False)

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator"):
        mock_lc.current.return_value = mock_launcher
        cw = CreateWriter(contract)
        cw._apply_column_masks()

    set_mask_calls = [
        c for c in mock_launcher.spark.sql.call_args_list
        if "SET MASK" in str(c)
    ]
    assert len(set_mask_calls) == 0


# ─────────────────────────────────────────────────────────────────────────────
# TC-MK02  _apply_column_masks ejecuta ALTER en Databricks
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_column_masks_executes_in_databricks():
    from DKOps.table_governance.writers.create_writer import CreateWriter

    contract      = _make_contract(masked_cols=True)
    mock_launcher = _mock_launcher(is_databricks=True)

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator"):
        mock_lc.current.return_value = mock_launcher
        cw = CreateWriter(contract)
        cw._apply_column_masks()

    set_mask_calls = [
        str(c) for c in mock_launcher.spark.sql.call_args_list
        if "SET MASK" in str(c)
    ]
    assert len(set_mask_calls) == 1
    assert "email" in set_mask_calls[0]
    assert "security.mask_email" in set_mask_calls[0]


# ─────────────────────────────────────────────────────────────────────────────
# TC-MK03  _apply_column_masks se omite en dry_run
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_column_masks_skipped_in_dry_run():
    from DKOps.table_governance.writers.create_writer import CreateWriter

    contract      = _make_contract(masked_cols=True)
    mock_launcher = _mock_launcher(is_databricks=True)

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator"):
        mock_lc.current.return_value = mock_launcher
        cw = CreateWriter(contract, dry_run=True)
        cw._apply_column_masks()

    set_mask_calls = [
        c for c in mock_launcher.spark.sql.call_args_list
        if "SET MASK" in str(c)
    ]
    assert len(set_mask_calls) == 0


# ─────────────────────────────────────────────────────────────────────────────
# TC-MK04  tabla sin masks: _apply_column_masks es no-op
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_column_masks_noop_when_no_masks():
    from DKOps.table_governance.writers.create_writer import CreateWriter

    contract      = _make_contract(masked_cols=False)
    mock_launcher = _mock_launcher(is_databricks=True)

    with patch("DKOps.table_governance.writers.base_writer.Launcher") as mock_lc, \
         patch("DKOps.table_governance.contracts.validator.SchemaValidator"):
        mock_lc.current.return_value = mock_launcher
        cw = CreateWriter(contract)
        cw._apply_column_masks()

    set_mask_calls = [
        c for c in mock_launcher.spark.sql.call_args_list
        if "SET MASK" in str(c)
    ]
    assert len(set_mask_calls) == 0
