"""
test_safe_migrator.py
=====================
Tests para SafeMigrator y MigrationPlan.

No requiere Spark real — usa mocks de pyspark y Launcher.

Ejecutar:
    pytest tests/test_safe_migrator.py -v
"""

import sys
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
from DKOps.table_governance.migrations.safe_migrator import (
    MigrationOp,
    MigrationPlan,
    SafeMigrator,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_contract(
    extra_cols: bool = False,
    with_comment: bool = False,
    with_properties: bool = False,
    with_permissions: bool = False,
) -> TableContract:
    from DKOps.table_governance.contracts.loader import (
        ColumnContract, TableContract, PermissionContract,
    )

    cols = [
        ColumnContract(name="id",    type="STRING",  nullable=False, comment="Clave primaria"),
        ColumnContract(name="valor", type="INTEGER", nullable=True),
    ]
    if extra_cols:
        cols.append(ColumnContract(name="nuevo_campo", type="DOUBLE", nullable=True, comment="Campo nuevo"))

    perms = []
    if with_permissions:
        perms = [PermissionContract(operation="GRANT", action="SELECT", principal="analysts")]

    return TableContract(
        catalog     = "ct_bronze_dev",
        schema      = "test_schema",
        name        = "test_table",
        type        = "MANAGED",
        format      = "DELTA",
        columns     = tuple(cols),
        comment     = "Tabla de prueba" if with_comment else None,
        properties  = {"delta.enableChangeDataFeed": "true"} if with_properties else {},
        permissions = tuple(perms),
    )


def _mock_launcher(is_databricks: bool = False):
    launcher = MagicMock()
    launcher.env._is_databricks = is_databricks
    launcher.env.env = "dev"
    launcher.spark   = MagicMock()
    return launcher


def _patch_launcher(is_databricks: bool = False):
    mock = _mock_launcher(is_databricks)
    return patch(
        "DKOps.table_governance.migrations.safe_migrator.Launcher",
        **{"current.return_value": mock},
    ), mock


# ─────────────────────────────────────────────────────────────────────────────
# TC-MP01  MigrationPlan.is_empty
# ─────────────────────────────────────────────────────────────────────────────

def test_migration_plan_empty():
    plan = MigrationPlan(table="ct.s.t")
    assert plan.is_empty is True


def test_migration_plan_not_empty():
    plan = MigrationPlan(table="ct.s.t", operations=[
        MigrationOp(kind="add_column", description="col x", sql="ALTER TABLE ...")
    ])
    assert plan.is_empty is False


# ─────────────────────────────────────────────────────────────────────────────
# TC-MP02  MigrationPlan.print() no lanza cuando está vacío
# ─────────────────────────────────────────────────────────────────────────────

def test_migration_plan_print_empty(capsys):
    MigrationPlan(table="ct.s.t").print()
    out = capsys.readouterr().out
    assert "Sin cambios" in out


def test_migration_plan_print_operations(capsys):
    plan = MigrationPlan(table="ct.s.t", operations=[
        MigrationOp(kind="add_column", description="nueva col x", sql="ALTER TABLE ct.s.t ADD COLUMN x STRING"),
    ])
    plan.print()
    out = capsys.readouterr().out
    assert "add_column" in out.lower() or "ADD_COLUMN" in out
    assert "ALTER TABLE" in out


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM01  plan() devuelve plan vacío cuando la tabla no existe
# ─────────────────────────────────────────────────────────────────────────────

def test_plan_empty_when_table_not_exists():
    contract = _make_contract()
    patcher, mock = _patch_launcher()

    with patcher:
        mock.spark.sql.side_effect = Exception("Table not found")
        migrator = SafeMigrator(contract)
        plan = migrator.plan()

    assert plan.is_empty


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM02  plan() detecta columnas nuevas vs las existentes
# ─────────────────────────────────────────────────────────────────────────────

def test_plan_detects_new_columns():
    contract = _make_contract(extra_cols=True)
    patcher, mock = _patch_launcher()

    existing_row_id    = MagicMock(); existing_row_id.__getitem__ = lambda s, i: ["id",    "STRING",  ""][i]
    existing_row_valor = MagicMock(); existing_row_valor.__getitem__ = lambda s, i: ["valor","INTEGER",""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [
            existing_row_id, existing_row_valor,
        ]
        migrator = SafeMigrator(contract)
        plan = migrator.plan()

    add_ops = [op for op in plan.operations if op.kind == "add_column"]
    assert len(add_ops) == 1
    assert "nuevo_campo" in add_ops[0].sql


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM03  plan() detecta comentarios de columna que cambiaron
# ─────────────────────────────────────────────────────────────────────────────

def test_plan_detects_changed_column_comment():
    contract = _make_contract()
    patcher, mock = _patch_launcher()

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  "comentario_viejo"][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract)
        plan = migrator.plan()

    comment_ops = [op for op in plan.operations if op.kind == "change_comment"]
    assert len(comment_ops) == 1
    assert "id" in comment_ops[0].sql


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM04  plan() en Databricks incluye permisos
# ─────────────────────────────────────────────────────────────────────────────

def test_plan_includes_permissions_on_databricks():
    contract = _make_contract(with_permissions=True)
    patcher, mock = _patch_launcher(is_databricks=True)

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  ""][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract, dry_run=True)
        plan = migrator.plan()

    perm_ops = [op for op in plan.operations if op.kind == "permission"]
    assert len(perm_ops) == 1
    assert "analysts" in perm_ops[0].sql


def test_plan_skips_permissions_on_local():
    contract = _make_contract(with_permissions=True)
    patcher, mock = _patch_launcher(is_databricks=False)

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  ""][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract)
        plan = migrator.plan()

    perm_ops = [op for op in plan.operations if op.kind == "permission"]
    assert len(perm_ops) == 0


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM05  apply() con dry_run no ejecuta SQL
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_dry_run_does_not_execute_sql():
    contract = _make_contract(extra_cols=True)
    patcher, mock = _patch_launcher()

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  ""][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract, dry_run=True)
        plan = migrator.apply()

    executed_sqls = [str(c) for c in mock.spark.sql.call_args_list]
    alter_calls   = [s for s in executed_sqls if "ADD COLUMN" in s or "ALTER COLUMN" in s]
    assert len(alter_calls) == 0, f"dry_run no debería ejecutar ALTER. Calls: {executed_sqls}"


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM06  apply() sin dry_run ejecuta cada operación del plan
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_executes_operations():
    contract = _make_contract(extra_cols=True)
    patcher, mock = _patch_launcher()

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  ""][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract, dry_run=False)
        plan = migrator.apply()

    add_ops = [op for op in plan.operations if op.kind == "add_column"]
    assert len(add_ops) == 1

    executed_sqls = " ".join(str(c) for c in mock.spark.sql.call_args_list)
    assert "ADD COLUMN" in executed_sqls


# ─────────────────────────────────────────────────────────────────────────────
# TC-SM07  apply() tabla sin cambios devuelve plan vacío
# ─────────────────────────────────────────────────────────────────────────────

def test_apply_no_ops_when_table_up_to_date():
    contract = _make_contract()
    patcher, mock = _patch_launcher()

    row_id    = MagicMock(); row_id.__getitem__    = lambda s, i: ["id",    "STRING",  "Clave primaria"][i]
    row_valor = MagicMock(); row_valor.__getitem__ = lambda s, i: ["valor", "INTEGER", ""][i]

    with patcher:
        mock.spark.sql.return_value.collect.return_value = [row_id, row_valor]
        migrator = SafeMigrator(contract)
        plan = migrator.apply()

    assert plan.is_empty
