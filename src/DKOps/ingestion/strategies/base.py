"""base.py — BasePromotionStrategy: contrato abstracto para estrategias Silver."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import TableContract
from DKOps.table_governance.writers.table_writer import TableWriter
from DKOps.table_governance.readers.table_reader import TableReader


class BasePromotionStrategy(ABC, LoggableMixin):
    """
    Estrategia de promoción Bronze → Silver.

    Subclases implementan `execute()` con la lógica específica:
    - FullMerge: dedup por clave negocio, mantiene registro más reciente
    - CdcMerge: aplica eventos I/U/D con soft deletes
    - IncrementalReplace: reemplaza partición actual con snapshot nuevo
    - AppendDedup: inserta solo si la clave no existe en destino
    """

    def __init__(
        self,
        spark:        SparkSession,
        contract:     IngestionContract,
        src_contract: TableContract,
        dst_contract: TableContract,
    ) -> None:
        self._spark        = spark
        self._contract     = contract
        self._src_contract = src_contract
        self._dst_contract = dst_contract
        self._reader       = TableReader(src_contract)
        self._writer       = TableWriter(dst_contract)

    @abstractmethod
    def execute(self) -> int:
        """
        Ejecuta la estrategia de promoción.
        Devuelve el número de filas escritas en Silver.
        """
        ...

    def _read_bronze(self, filter_expr: str | None = None) -> DataFrame:
        """Lee Bronze aplicando el filtro del contrato si existe."""
        f = filter_expr or self._contract.data_filter
        return self._reader.read(filter=f) if f else self._reader.read()

    @property
    def strategy_name(self) -> str:
        return self.__class__.__name__
