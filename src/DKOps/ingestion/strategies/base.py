"""base.py — BasePromotionStrategy: contrato abstracto para estrategias Silver."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

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
        self._reader       = TableReader(src_contract)   # lector de Bronze (fuente)
        self._dst_reader   = TableReader(dst_contract)   # lector de Silver (destino)
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

    def _add_silver_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Añade las columnas técnicas de Silver si el contrato de ingesta las pide.

        Añade **ambas** columnas, igual que MetadataEnricher:

          _silver_created_at    primera escritura de la fila en Silver
          _silver_modified_at   última modificación de la fila en Silver

        Solo se añaden las que el contrato Silver declare — `_select_for_silver()`
        descarta el resto, así que un contrato que solo declare
        `_silver_modified_at` sigue funcionando igual que antes.

        Preservar `_silver_created_at` entre ejecuciones es responsabilidad de
        la estrategia: las que hacen MERGE deben pasarla en `insert_only_columns`
        para que el UPDATE no la sobrescriba.
        """
        if not self._contract.metadata.add_silver_timestamps:
            return df

        now = F.current_timestamp()
        return (
            df.withColumn("_silver_created_at",  now)
              .withColumn("_silver_modified_at", now)
        )

    def _silver_insert_only_columns(self) -> list[str]:
        """
        Columnas que un MERGE debe insertar pero nunca actualizar.

        `_silver_created_at` marca la primera inserción: si el UPDATE la
        reescribiera con `current_timestamp()` en cada ejecución dejaría de
        significar nada.
        """
        if not self._contract.metadata.add_silver_timestamps:
            return []
        return [c for c in ("_silver_created_at",) if c in self._dst_contract.column_names]

    def _select_for_silver(self, df: DataFrame) -> DataFrame:
        """
        Selecciona únicamente las columnas declaradas en el contrato Silver.

        Evita que columnas de metadatos de Bronze (_ingested_at, _ingested_date,
        _source_file) se propaguen a Silver, donde no están declaradas.
        Las columnas del contrato Silver que no estén en el DF se omiten
        silenciosamente (pueden ser añadidas por _apply_defaults en el writer).
        """
        silver_cols = self._dst_contract.column_names
        df_cols     = set(df.columns)
        cols        = [c for c in silver_cols if c in df_cols]
        return df.select(*cols)

    @property
    def strategy_name(self) -> str:
        return self.__class__.__name__
