"""
validate_contracts.py — valida los contratos JSON del repo contra schema/.

Valida sin arrancar Spark ni Databricks: solo lee JSON. Por eso sirve como
chequeo rápido en CI y como red de seguridad al escribir contratos nuevos.

Dos capas de validación:
  1. JSON Schema (schema/table_contract.schema.json, schema/ingestion_contract.schema.json)
     — requiere `pip install jsonschema`. Si no está instalado se omite con aviso.
  2. Chequeos cruzados que un JSON Schema no puede expresar:
     - columnas de 'partitions' / 'clustering' existen en 'columns'
     - nombres de columna sin duplicados
     - las rutas de 'destination_contract' / 'source_contract' apuntan a un archivo real

Uso
---
    python scripts/validate_contracts.py              # valida demos/
    python scripts/validate_contracts.py demos/demo_5 # valida un demo concreto

Salida: exit code 0 si todo está bien, 1 si hay al menos un error.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT        = Path(__file__).resolve().parents[1]
SCHEMA_DIR  = ROOT / "schema"
DEFAULT_DIR = ROOT / "demos"


# ── Carga de schemas ─────────────────────────────────────────────────────────

def _load_schemas() -> tuple[dict, dict]:
    table = json.loads((SCHEMA_DIR / "table_contract.schema.json").read_text("utf-8"))
    ingestion = json.loads(
        (SCHEMA_DIR / "ingestion_contract.schema.json").read_text("utf-8")
    )
    return table, ingestion


def _get_validator():
    """Devuelve una función (data, schema) -> list[str] de errores, o None."""
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        return None

    def validate(data: dict, schema: dict) -> list[str]:
        v = Draft202012Validator(schema)
        errors = []
        for err in sorted(v.iter_errors(data), key=lambda e: list(e.path)):
            loc = ".".join(str(p) for p in err.path) or "(raíz)"
            errors.append(f"{loc}: {err.message}")
        return errors

    return validate


# ── Chequeos cruzados ────────────────────────────────────────────────────────

def _check_table_contract(data: dict, path: Path) -> list[str]:
    errors: list[str] = []
    columns = data.get("columns") or []
    names   = [c.get("name") for c in columns if isinstance(c, dict)]

    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        errors.append(f"columnas duplicadas: {sorted(dupes)}")

    known = set(names)
    for p in data.get("partitions", []):
        if p not in known:
            errors.append(f"partición '{p}' no está declarada en 'columns'")

    clustering = data.get("clustering") or {}
    for c in clustering.get("columns", []):
        if c not in known:
            errors.append(f"columna de clustering '{c}' no está declarada en 'columns'")

    return errors


def _check_ingestion_contract(data: dict, path: Path) -> list[str]:
    errors: list[str] = []

    for field in ("destination_contract", "source_contract"):
        rel = data.get(field)
        if not rel:
            continue
        target = (path.parent / rel).resolve()
        if not target.is_file():
            errors.append(f"{field} apunta a un archivo inexistente: {rel}")

    # Un contrato de Silver que enriquece con metadatos de Bronze es casi
    # siempre un copy-paste del contrato batch.
    if data.get("strategy") and data.get("metadata", {}).get("add_source_file"):
        errors.append(
            "promoción Silver con metadata.add_source_file=true — "
            "_source_file es una columna de Bronze, no de Silver"
        )

    return errors


# ── Recorrido ────────────────────────────────────────────────────────────────

def _display_path(file: Path) -> str:
    """Ruta relativa al repo cuando se pueda; absoluta si el archivo está fuera."""
    try:
        return str(file.relative_to(ROOT))
    except ValueError:
        return str(file)


def _collect(base: Path) -> list[tuple[Path, str]]:
    """Devuelve (archivo, tipo) para cada contrato encontrado bajo `base`."""
    found: list[tuple[Path, str]] = []
    for json_file in sorted(base.rglob("*.json")):
        parts = json_file.relative_to(base).parts
        if "tables" in parts:
            found.append((json_file, "table"))
        elif "ingestion" in parts:
            found.append((json_file, "ingestion"))
        # config/*.json y cualquier otro JSON se ignoran a propósito
    return found


def main(argv: list[str]) -> int:
    base = Path(argv[1]).resolve() if len(argv) > 1 else DEFAULT_DIR
    if not base.is_dir():
        print(f"ERROR: no es un directorio: {base}")
        return 1

    table_schema, ingestion_schema = _load_schemas()
    schema_validate = _get_validator()
    if schema_validate is None:
        print("AVISO: 'jsonschema' no instalado — se omite la validación de schema.")
        print("       pip install jsonschema\n")

    contracts = _collect(base)
    if not contracts:
        print(f"No se encontraron contratos bajo {base}")
        return 0

    failures = 0
    for file, kind in contracts:
        rel = _display_path(file)
        try:
            data = json.loads(file.read_text("utf-8"))
        except json.JSONDecodeError as exc:
            print(f"FAIL {rel}\n       JSON inválido: {exc}")
            failures += 1
            continue

        errors: list[str] = []
        if schema_validate:
            schema = table_schema if kind == "table" else ingestion_schema
            errors += schema_validate(data, schema)
        errors += (
            _check_table_contract(data, file)
            if kind == "table"
            else _check_ingestion_contract(data, file)
        )

        if errors:
            failures += 1
            print(f"FAIL {rel}  [{kind}]")
            for e in errors:
                print(f"       {e}")
        else:
            print(f"ok   {rel}  [{kind}]")

    total = len(contracts)
    print(f"\n{total - failures}/{total} contratos válidos")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
