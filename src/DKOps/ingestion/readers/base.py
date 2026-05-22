"""base.py — Contrato abstracto para todos los lectores de fuentes."""

from __future__ import annotations

from abc import ABC, abstractmethod

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.logger_config import LoggableMixin


class BaseSourceReader(ABC, LoggableMixin):
    """
    Lector abstracto de fuentes de datos.

    Cada implementación concreta sabe leer desde un tipo de fuente
    (archivos locales, Auto Loader, Kafka, etc.) y devuelve un DataFrame
    —batch o streaming— sin conocer cómo se escribe el destino.
    """

    def __init__(self, contract: IngestionContract) -> None:
        self.contract = contract

    @abstractmethod
    def read(self):
        """
        Lee la fuente y devuelve un DataFrame.
        - Batch readers: DataFrame estático.
        - Streaming readers: DataFrame de streaming (isStreaming=True).
        """
        ...

    @property
    def is_streaming(self) -> bool:
        return self.contract.is_streaming()

    @property
    def source_format(self) -> str:
        return self.contract.source.format
