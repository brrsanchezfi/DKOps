"""
test_contracts.py
=================
Tests para las nuevas funcionalidades del módulo de contratos:
  - merge_schema en TableContract
  - mask en ColumnContract
  - TableWriter facade

No requiere Spark real — usa mocks donde es necesario.

Ejecutar:
    pytest tests/test_contracts.py -v
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

# Ajuste de path para imports sin instalar el paquete
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# Mock de pyspark — table_governance/__init__.py importa los writers que dependen de Spark.
# Lo hacemos antes de cualquier import de DKOps para que los tests de contratos
# puedan correr sin PySpark instalado.
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions",
    "pyspark.sql.types", "pyspark.sql.dataframe",
    "delta", "delta.tables",
]:
    sys.modules.setdefault(_mod, MagicMock())

from DKOps.table_governance.contracts.loader import (
    ColumnContract,
    TableContract,
    ContractLoader,
    load_contract,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _minimal_contract_dict(**overrides) -> dict:
    base = {
        "catalog":  "ct_bronze_dev",
        "schema":   "test_schema",
        "name":     "test_table",
        "columns":  [
            {"name": "id",    "type": "STRING", "nullable": False},
            {"name": "valor", "type": "INTEGER"},
        ],
    }
    base.update(overrides)
    return base


def _write_json(tmp_dir: str, data: dict, filename: str = "contract.json") -> str:
    path = Path(tmp_dir) / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def _make_loader(tmp_dir: str) -> ContractLoader:
    """
    ContractLoader con un EnvironmentConfig mínimo mockeado.
    Evita la necesidad de un Launcher activo en los tests.
    """
    mock_env = MagicMock()
    mock_env.env       = "dev"
    mock_env.env_short = "d"
    mock_env._vars     = {"catalogs": {}, "paths": {}}
    mock_env._is_databricks = False
    return ContractLoader(env=mock_env)


# ─────────────────────────────────────────────────────────────────────────────
# TC-C01  merge_schema default es False
# ─────────────────────────────────────────────────────────────────────────────

def test_merge_schema_default_false():
    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, _minimal_contract_dict())
        ct     = loader.load(path)

    assert ct.merge_schema is False


# ─────────────────────────────────────────────────────────────────────────────
# TC-C02  merge_schema: true se parsea correctamente
# ─────────────────────────────────────────────────────────────────────────────

def test_merge_schema_true():
    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        data   = _minimal_contract_dict(merge_schema=True)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    assert ct.merge_schema is True


# ─────────────────────────────────────────────────────────────────────────────
# TC-C03  merge_schema: false explícito
# ─────────────────────────────────────────────────────────────────────────────

def test_merge_schema_false_explicit():
    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        data   = _minimal_contract_dict(merge_schema=False)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    assert ct.merge_schema is False


# ─────────────────────────────────────────────────────────────────────────────
# TC-C04  mask default es None en ColumnContract
# ─────────────────────────────────────────────────────────────────────────────

def test_column_mask_default_none():
    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, _minimal_contract_dict())
        ct     = loader.load(path)

    for col in ct.columns:
        assert col.mask is None
        assert col.has_mask is False


# ─────────────────────────────────────────────────────────────────────────────
# TC-C05  mask se parsea en columnas que lo declaran
# ─────────────────────────────────────────────────────────────────────────────

def test_column_mask_parsed():
    data = _minimal_contract_dict()
    data["columns"] = [
        {"name": "id",    "type": "STRING"},
        {"name": "email", "type": "STRING", "mask": "security.mask_email"},
        {"name": "cedula","type": "STRING", "mask": "security.mask_id"},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    id_col     = ct.get_column("id")
    email_col  = ct.get_column("email")
    cedula_col = ct.get_column("cedula")

    assert id_col.mask     is None
    assert id_col.has_mask is False

    assert email_col.mask     == "security.mask_email"
    assert email_col.has_mask is True

    assert cedula_col.mask     == "security.mask_id"
    assert cedula_col.has_mask is True


# ─────────────────────────────────────────────────────────────────────────────
# TC-C06  masked_columns filtra solo columnas con mask
# ─────────────────────────────────────────────────────────────────────────────

def test_masked_columns_property():
    data = _minimal_contract_dict()
    data["columns"] = [
        {"name": "id",    "type": "STRING"},
        {"name": "email", "type": "STRING", "mask": "security.mask_email"},
        {"name": "nombre","type": "STRING"},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    masked = ct.masked_columns
    assert len(masked) == 1
    assert masked[0].name == "email"


# ─────────────────────────────────────────────────────────────────────────────
# TC-C07  mask vacío ("") se normaliza a None
# ─────────────────────────────────────────────────────────────────────────────

def test_column_mask_empty_string_normalized_to_none():
    data = _minimal_contract_dict()
    data["columns"] = [
        {"name": "id",    "type": "STRING"},
        {"name": "email", "type": "STRING", "mask": ""},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    assert ct.get_column("email").mask is None


# ─────────────────────────────────────────────────────────────────────────────
# TC-C08  merge_schema es inmutable en el dataclass frozen
# ─────────────────────────────────────────────────────────────────────────────

def test_table_contract_is_frozen():
    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        data   = _minimal_contract_dict(merge_schema=True)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    with pytest.raises(Exception):  # FrozenInstanceError
        ct.merge_schema = False  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────────────────────
# TC-C09  merge_schema + mask coexisten en el mismo contrato
# ─────────────────────────────────────────────────────────────────────────────

def test_merge_schema_and_mask_together():
    data = _minimal_contract_dict(merge_schema=True)
    data["columns"] = [
        {"name": "id",    "type": "STRING"},
        {"name": "email", "type": "STRING", "mask": "sec.mask_email"},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        loader = _make_loader(tmp)
        path   = _write_json(tmp, data)
        ct     = loader.load(path)

    assert ct.merge_schema is True
    assert ct.get_column("email").mask == "sec.mask_email"
    assert len(ct.masked_columns) == 1
