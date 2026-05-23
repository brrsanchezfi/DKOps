"""
ingestion_contract.py
=====================
Contratos de ingesta: fuente, destino, tipo de carga y metadata.

Un IngestionContract conecta:
  - Una fuente (archivos en Landing o tópico Kafka)
  - Un TableContract destino (Bronze/Silver, gobernado por DKOps)
  - Reglas de enriquecimiento de metadata técnica
  - Configuración de trigger y checkpoint para streaming

Todos los contratos se cargan desde JSON vía IngestionContractLoader.

Ejemplo batch JSON:
    {
      "name": "ventas_diarias",
      "ingest_type": "batch",
      "load_type": "incremental",
      "source": {
        "format": "json",
        "path": "{path.landing}/ventas_diarias"
      },
      "destination_contract": "../../tables/bronze/ventas_raw.json",
      "metadata": { "add_ingested_at": true, "add_source_file": true }
    }

Ejemplo streaming JSON:
    {
      "name": "eventos_app",
      "ingest_type": "streaming",
      "trigger": "available_now",
      "source": { "format": "json", "path": "{path.landing}/eventos_app" },
      "destination_contract": "../../tables/bronze/eventos_app_raw.json"
    }

Ejemplo silver promotion JSON:
    {
      "name": "ventas_current",
      "ingest_type": "batch",
      "strategy": "cdc_merge",
      "source_contract": "../../tables/bronze/ventas_raw.json",
      "destination_contract": "../../tables/silver/ventas_current.json",
      "merge_keys": ["venta_id"],
      "watermark_col": "fecha_venta"
    }
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class IngestionType(str, Enum):
    BATCH     = "batch"
    STREAMING = "streaming"


class LoadType(str, Enum):
    FULL        = "full"
    INCREMENTAL = "incremental"
    CDC         = "cdc"
    STREAMING   = "streaming"   # alias semántico para contratos streaming


class StreamTrigger(str, Enum):
    AVAILABLE_NOW = "available_now"   # batch-style: procesa pendientes y para
    CONTINUOUS    = "continuous"       # continuo: no bloquea, requiere stop explícito


class SilverStrategy(str, Enum):
    FULL_MERGE          = "full_merge"
    CDC_MERGE           = "cdc_merge"
    INCREMENTAL_REPLACE = "incremental_replace"
    APPEND_DEDUP        = "append_dedup"


@dataclass(frozen=True)
class SourceSpec:
    """Especificación de la fuente de datos (Landing o Kafka)."""
    format:  str
    path:    str | None          = None
    kafka:   dict                = field(default_factory=dict)
    options: dict                = field(default_factory=dict)
    schema:  tuple[dict, ...]    = field(default_factory=tuple)


@dataclass(frozen=True)
class MetadataConfig:
    """Columnas técnicas que MetadataEnricher añade al DataFrame."""
    add_ingested_at:       bool = True
    add_ingested_date:     bool = True
    add_source_file:       bool = True
    add_kafka_metadata:    bool = False
    add_silver_timestamps: bool = False


@dataclass(frozen=True)
class IngestionContract:
    """
    Contrato de ingesta — inmutable y tipado.
    Construido por IngestionContractLoader. No instanciar directamente.
    """
    name:                      str
    ingest_type:               IngestionType
    source:                    SourceSpec
    destination_contract_path: str
    metadata:                  MetadataConfig
    checkpoint_suffix:         str

    description:          str             = ""
    enabled:              bool            = True
    load_type:            LoadType        = LoadType.INCREMENTAL
    trigger:              StreamTrigger   = StreamTrigger.AVAILABLE_NOW

    # Silver promotion
    strategy:             SilverStrategy | None = None
    source_contract_path: str | None            = None
    merge_keys:           tuple[str, ...]        = field(default_factory=tuple)
    watermark_col:        str | None            = None
    data_filter:          str | None            = None

    source_path_resolved: str = ""

    def is_streaming(self) -> bool:
        return self.ingest_type == IngestionType.STREAMING

    def is_batch(self) -> bool:
        return self.ingest_type == IngestionType.BATCH

    def is_silver_promotion(self) -> bool:
        return self.strategy is not None

    def __repr__(self) -> str:
        return (
            f"IngestionContract({self.name!r}, "
            f"type={self.ingest_type.value}, "
            f"strategy={self.strategy.value if self.strategy else 'none'})"
        )
