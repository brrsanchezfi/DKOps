"""
silver_promoter.py — Motor de promoción Bronze → Silver.

Selecciona y ejecuta la estrategia de promoción correcta según el contrato:

  full_merge           → FullMergeStrategy
  cdc_merge            → CdcMergeStrategy
  incremental_replace  → IncrementalReplaceStrategy
  append_dedup         → AppendDedupStrategy

Las estrategias usan TableReader (Bronze) y TableWriter (Silver) del módulo
de gobernanza existente — el ingestion engine no escribe directamente.

Las lakehouse applications (demos, pipelines de negocio) son independientes:
leen de Silver con TableReader y aplican su propia lógica de negocio.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from DKOps.environment_config import EnvironmentConfig
from DKOps.ingestion.contracts.ingestion_contract import IngestionContract, SilverStrategy
from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
from DKOps.ingestion.strategies.append_dedup import AppendDedupStrategy
from DKOps.ingestion.strategies.base import BasePromotionStrategy
from DKOps.ingestion.strategies.cdc_merge import CdcMergeStrategy
from DKOps.ingestion.strategies.full_merge import FullMergeStrategy
from DKOps.ingestion.strategies.incremental_replace import IncrementalReplaceStrategy
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import TableContract


class SilverPromoter(LoggableMixin):
    """
    Promueve datos desde Bronze hacia Silver aplicando la estrategia del contrato.

    Uso
    ---
        promoter = SilverPromoter(spark, env, ops)
        rows = promoter.promote(contract, src_contract, dst_contract)
    """

    def __init__(
        self,
        spark: SparkSession,
        env:   EnvironmentConfig,
        ops:   IngestionOpsLogger | None = None,
    ) -> None:
        self._spark = spark
        self._env   = env
        self._ops   = ops

    def promote(
        self,
        contract:     IngestionContract,
        src_contract: TableContract,
        dst_contract: TableContract,
    ) -> int:
        """
        Ejecuta la promoción de un dataset.

        Parámetros
        ----------
        contract     : IngestionContract con la estrategia y config
        src_contract : TableContract de la tabla Bronze (fuente)
        dst_contract : TableContract de la tabla Silver (destino)
        """
        if contract.strategy is None:
            raise ValueError(
                f"[{contract.name}] SilverPromoter requiere 'strategy' en el contrato."
            )

        run_id = self._ops.log_start(f"silver.{contract.name}") if self._ops else "local"

        try:
            strategy = self._build_strategy(contract, src_contract, dst_contract)
            rows     = strategy.execute()

            if self._ops:
                self._ops.log_success(
                    run_id, f"silver.{contract.name}", rows_written=rows
                )

            return rows

        except Exception as exc:
            if self._ops:
                self._ops.log_failure(run_id, f"silver.{contract.name}", exc)
            raise

    def promote_all(
        self,
        contracts:     list[IngestionContract],
        src_contracts: dict[str, TableContract],
        dst_contracts: dict[str, TableContract],
    ) -> list[str]:
        """
        Promueve múltiples datasets. Devuelve lista de nombres que fallaron.
        """
        failed = []
        for c in contracts:
            src = src_contracts.get(c.name)
            dst = dst_contracts.get(c.name)
            if src is None or dst is None:
                self.log.warning(f"[{c.name}] Falta src o dst contract — omitido")
                continue
            try:
                self.promote(c, src, dst)
            except Exception as exc:
                self.log.error(f"[{c.name}] Error en promoción: {exc} — continuando")
                failed.append(c.name)

        if failed:
            self.log.warning(f"Datasets Silver fallidos: {failed}")
        else:
            self.log.info("Promoción Silver completada sin errores ✔")
        return failed

    def _build_strategy(
        self,
        contract:     IngestionContract,
        src_contract: TableContract,
        dst_contract: TableContract,
    ) -> BasePromotionStrategy:
        kwargs = dict(
            spark        = self._spark,
            contract     = contract,
            src_contract = src_contract,
            dst_contract = dst_contract,
        )

        strategy_map: dict[SilverStrategy, type[BasePromotionStrategy]] = {
            SilverStrategy.FULL_MERGE:          FullMergeStrategy,
            SilverStrategy.INCREMENTAL_REPLACE: IncrementalReplaceStrategy,
            SilverStrategy.APPEND_DEDUP:        AppendDedupStrategy,
        }

        if contract.strategy in strategy_map:
            return strategy_map[contract.strategy](**kwargs)

        if contract.strategy == SilverStrategy.CDC_MERGE:
            return CdcMergeStrategy(**kwargs)

        raise ValueError(
            f"Estrategia '{contract.strategy}' no reconocida. "
            f"Válidas: {[s.value for s in SilverStrategy]}"
        )
