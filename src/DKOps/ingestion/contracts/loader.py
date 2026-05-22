"""
loader.py — IngestionContractLoader: carga contratos de ingesta desde JSON.

Resuelve los mismos placeholders que ContractLoader ({path.landing}, etc.)
y carga el TableContract destino/fuente referenciado en el JSON.

Uso
---
    loader = IngestionContractLoader(
        contracts_dir="demos/demo_5/ingestion/batch",
        base_dir="demos/demo_5",
    )
    contracts = loader.load_all()
    dst = loader.load_destination(contracts[0])
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from DKOps.environment_config import EnvironmentConfig
from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract,
    IngestionType,
    LoadType,
    MetadataConfig,
    SilverStrategy,
    SourceSpec,
    StreamTrigger,
)
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import ContractLoader, TableContract


class IngestionContractLoader(LoggableMixin):
    """
    Carga contratos de ingesta (IngestionContract) desde archivos JSON.

    Parámetros
    ----------
    contracts_dir : directorio con los JSON de ingesta.
    base_dir      : directorio base para resolver destination_contract paths.
                    Si no se pasa, se usa el parent de contracts_dir.
    env           : EnvironmentConfig. Si no se pasa, se obtiene del Launcher.
    """

    def __init__(
        self,
        contracts_dir: str | Path,
        base_dir:      str | Path | None = None,
        env:           EnvironmentConfig | None = None,
    ) -> None:
        self._contracts_dir = Path(contracts_dir)
        self._base_dir      = Path(base_dir) if base_dir else self._contracts_dir.parent
        self._env           = env or self._get_env()
        self._table_loader  = ContractLoader(self._env)

    def _get_env(self) -> EnvironmentConfig:
        from DKOps.launcher import Launcher
        return Launcher.current().env

    def load(self, path: str | Path) -> IngestionContract:
        path = Path(path)
        self.log.info(f"▶ Cargando contrato de ingesta: {path.name}")

        raw      = self._read_json(path)
        resolved = self._resolve_placeholders(raw)
        contract = self._build_contract(resolved, source_path=path)

        self.log.info(
            f"✔ {contract.name} | type={contract.ingest_type.value} | "
            f"enabled={contract.enabled}"
        )
        return contract

    def load_all(self) -> list[IngestionContract]:
        """Carga todos los .json del directorio. Omite los deshabilitados."""
        if not self._contracts_dir.is_dir():
            raise NotADirectoryError(f"No es un directorio: {self._contracts_dir}")

        contracts = []
        for json_file in sorted(self._contracts_dir.glob("*.json")):
            contract = self.load(json_file)
            if contract.enabled:
                contracts.append(contract)
            else:
                self.log.info(f"  ↳ Omitido (enabled=false): {contract.name}")

        self.log.info(f"Contratos cargados: {len(contracts)}")
        return contracts

    def load_destination(self, contract: IngestionContract) -> TableContract:
        """Carga el TableContract destino referenciado por el IngestionContract."""
        dest_path = (self._base_dir / contract.destination_contract_path).resolve()
        return self._table_loader.load(dest_path)

    def load_source(self, contract: IngestionContract) -> TableContract | None:
        """Carga el TableContract fuente (solo para Silver promotion)."""
        if not contract.source_contract_path:
            return None
        src_path = (self._base_dir / contract.source_contract_path).resolve()
        return self._table_loader.load(src_path)

    # ── Lectura JSON ──────────────────────────────────────────────────────

    def _read_json(self, path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"Contrato de ingesta no encontrado: {path}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ── Resolución de placeholders ────────────────────────────────────────

    def _resolve_placeholders(self, raw: dict) -> dict:
        ctx = self._build_context()
        return self._resolve_recursive(raw, ctx)

    def _build_context(self) -> dict[str, str]:
        ctx: dict[str, str] = {}
        for name, value in self._env._vars.get("catalogs", {}).items():
            ctx[f"catalog.{name}"] = value
        for name, value in self._env._vars.get("paths", {}).items():
            ctx[f"path.{name}"] = value
        ctx["env"]       = self._env.env
        ctx["env_short"] = self._env.env_short
        return ctx

    def _resolve_recursive(self, node: Any, ctx: dict[str, str]) -> Any:
        if isinstance(node, str):
            return self._resolve_string(node, ctx)
        if isinstance(node, dict):
            return {k: self._resolve_recursive(v, ctx) for k, v in node.items()}
        if isinstance(node, list):
            return [self._resolve_recursive(i, ctx) for i in node]
        return node

    @staticmethod
    def _resolve_string(value: str, ctx: dict[str, str]) -> str:
        pattern = re.compile(r"\{([^}]+)\}")

        def replacer(match: re.Match) -> str:
            key = match.group(1).strip()
            if key not in ctx:
                raise KeyError(
                    f"Placeholder '{{{key}}}' no reconocido.\n"
                    f"  Disponibles: {sorted(ctx.keys())}"
                )
            return ctx[key]

        return pattern.sub(replacer, value)

    # ── Construcción del contrato ─────────────────────────────────────────

    def _build_contract(self, data: dict, source_path: Path) -> IngestionContract:
        name = data.get("name", "").strip()
        if not name:
            raise ValueError(f"Campo 'name' obligatorio en {source_path}")

        ingest_type = IngestionType(data.get("ingest_type", "batch"))
        load_type   = LoadType(data.get("load_type", "incremental"))
        trigger     = StreamTrigger(data.get("trigger", "available_now"))

        src_data = data.get("source", {})
        source = SourceSpec(
            format  = src_data.get("format", "json"),
            path    = src_data.get("path"),
            kafka   = src_data.get("kafka", {}),
            options = src_data.get("options", {}),
            schema  = tuple(src_data.get("schema", [])),
        )

        meta_raw = data.get("metadata", {})
        metadata = MetadataConfig(
            add_ingested_at       = meta_raw.get("add_ingested_at",    True),
            add_ingested_date     = meta_raw.get("add_ingested_date",  True),
            add_source_file       = meta_raw.get("add_source_file",    True),
            add_kafka_metadata    = meta_raw.get("add_kafka_metadata", False),
            add_silver_timestamps = meta_raw.get("add_silver_timestamps", False),
        )

        dest_path = data.get("destination_contract", "")
        if not dest_path:
            raise ValueError(f"Campo 'destination_contract' obligatorio en {source_path}")

        raw_strategy = data.get("strategy")
        strategy = SilverStrategy(raw_strategy) if raw_strategy else None

        default_suffix = (
            f"streaming/{name}"
            if ingest_type == IngestionType.STREAMING
            else f"bronze/{name}"
        )

        return IngestionContract(
            name                      = name,
            description               = data.get("description", ""),
            ingest_type               = ingest_type,
            load_type                 = load_type,
            trigger                   = trigger,
            source                    = source,
            destination_contract_path = dest_path,
            metadata                  = metadata,
            checkpoint_suffix         = data.get("checkpoint_suffix", default_suffix),
            enabled                   = data.get("enabled", True),
            strategy                  = strategy,
            source_contract_path      = data.get("source_contract"),
            merge_keys                = tuple(data.get("merge_keys", [])),
            watermark_col             = data.get("watermark_col"),
            data_filter               = data.get("filter"),
            source_path_resolved      = str(source_path),
        )
