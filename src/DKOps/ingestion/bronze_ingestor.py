"""
bronze_ingestor.py — Motor de ingesta Landing → Bronze.

Orquesta el ciclo completo por dataset:
  1. Instancia el reader correcto (LocalBatch / AutoLoader / FileStream / Kafka)
  2. Enriquece con metadata técnica (MetadataEnricher)
  3. Valida el schema contra el TableContract destino (SchemaValidator)
  4. Escribe en Bronze via TableWriter (batch) o writeStream (streaming)
  5. Registra resultado en IngestionOpsLogger

Separación batch/streaming:
  - Batch   → spark.read() + TableWriter.append()
  - Streaming → spark.readStream + writeStream con trigger configurado
"""

from __future__ import annotations

import os
from datetime import date as _date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from DKOps.environment_config import EnvironmentConfig
from DKOps.ingestion.contracts.ingestion_contract import (
    IngestionContract, StreamTrigger,
)
from DKOps.ingestion.contracts.loader import IngestionContractLoader
from DKOps.ingestion.enrichment.metadata import MetadataEnricher
from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
from DKOps.ingestion.readers.factory import SourceReaderFactory
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import ContractLoader, TableContract
from DKOps.table_governance.contracts.validator import SchemaValidator
from DKOps.table_governance.writers.table_writer import TableWriter


class BronzeIngestor(LoggableMixin):
    """
    Ingesta datos desde Landing hasta Bronze.

    Uso
    ---
        ingestor = BronzeIngestor(spark, env, ops)
        rows = ingestor.ingest(contract, dst_contract)

    El BronzeIngestor no carga contratos — eso lo hace IngestionEngine.
    """

    def __init__(
        self,
        spark:       SparkSession,
        env:         EnvironmentConfig,
        ops:         IngestionOpsLogger | None = None,
        schema_root: str | None               = None,
        kafka_creds: dict | None              = None,
    ) -> None:
        self._spark       = spark
        self._env         = env
        self._ops         = ops
        self._schema_root = schema_root
        self._kafka_creds = kafka_creds
        self._enricher    = MetadataEnricher()

    def ingest(
        self,
        contract:     IngestionContract,
        dst_contract: TableContract,
    ) -> int:
        """
        Ejecuta la ingesta de un dataset.

        Parámetros
        ----------
        contract     : IngestionContract (fuente + config de ingesta)
        dst_contract : TableContract destino en Bronze

        Devuelve
        --------
        Número de filas escritas (estimado desde streaming o exacto desde batch).
        """
        run_id = self._ops.log_start(contract.name) if self._ops else "local"

        try:
            reader = SourceReaderFactory.create(
                contract    = contract,
                spark       = self._spark,
                env         = self._env,
                kafka_creds = self._kafka_creds,
                schema_root = self._schema_root,
            )

            df = reader.read()
            df = self._enricher.enrich(df, contract.metadata, contract.source.format)

            if df.isStreaming:
                rows = self._write_stream(df, contract, dst_contract)
            else:
                rows = self._write_batch(df, contract, dst_contract)

            if self._ops:
                self._ops.log_success(run_id, contract.name, rows_written=rows)

            return rows

        except Exception as exc:
            if self._ops:
                self._ops.log_failure(run_id, contract.name, exc)
            raise

    def ingest_all(
        self,
        contracts:     list[IngestionContract],
        dst_contracts: dict[str, TableContract],
    ) -> list[str]:
        """
        Ingesta múltiples datasets. Devuelve lista de nombres que fallaron.
        Continúa en caso de error por dataset (fail-continue).
        """
        failed = []
        for c in contracts:
            dst = dst_contracts.get(c.name)
            if dst is None:
                self.log.warning(f"[{c.name}] Sin TableContract destino — omitido")
                continue
            try:
                self.ingest(c, dst)
            except Exception as exc:
                self.log.error(f"[{c.name}] Error: {exc} — continuando")
                failed.append(c.name)

        if failed:
            self.log.warning(f"Datasets fallidos: {failed}")
        else:
            self.log.info("Ingesta batch completada sin errores ✔")
        return failed

    # ── Escritura batch ───────────────────────────────────────────────────

    def _write_batch(
        self,
        df:           DataFrame,
        contract:     IngestionContract,
        dst_contract: TableContract,
    ) -> int:
        """
        Escribe en Bronze usando *partition overwrite* sobre la columna de
        ingesta declarada en el contrato (por defecto ``_ingested_date``).

        Patrón: cada corrida sobreescribe únicamente la partición del día
        actual, dejando el histórico de otras particiones intacto. Esto
        garantiza idempotencia: ejecutar el motor N veces al día siempre
        deja la versión más reciente de los datos en esa partición.

        Si la tabla Bronze no declara ninguna columna de partición de ingesta,
        se usa ``append`` como fallback (con advertencia).

        El ``load_type`` del contrato (FULL / INCREMENTAL / CDC) es una
        etiqueta semántica para Silver — no afecta el mecanismo de escritura
        en Bronze.
        """
        validator = SchemaValidator(dst_contract)
        validator.validate(df).raise_if_critical()

        writer         = TableWriter(dst_contract, strict_columns=False)
        partition_col  = self._find_ingestion_partition(dst_contract)

        if partition_col and partition_col in df.columns:
            # Partition overwrite idempotente — solo la ventana de hoy
            today = str(_date.today())
            writer.overwrite_partition(df, {partition_col: today})
            self.log.info(
                f"[{contract.name}] partition overwrite OK | "
                f"table={dst_contract.full_name} | "
                f"{partition_col}={today} | load_type={contract.load_type.value}"
            )
        else:
            self.log.warning(
                f"[{contract.name}] Sin columna de partición de ingesta en el "
                f"TableContract → usando append. Considera añadir '_ingested_date' "
                f"a partition_columns en {dst_contract.name}.json"
            )
            writer.append(df)

        count = df.count()
        self.log.info(
            f"[{contract.name}] rows={count:,} | table={dst_contract.full_name}"
        )
        return count

    @staticmethod
    def _find_ingestion_partition(dst_contract: TableContract) -> str | None:
        """
        Devuelve la columna de partición de ingesta del contrato Bronze.

        Busca en orden: ``_ingested_date`` → ``_ingested_at``.
        Si ninguna está declarada como columna de partición, devuelve None.
        """
        for candidate in ("_ingested_date", "_ingested_at"):
            if candidate in dst_contract.partition_columns:
                return candidate
        return None

    # ── Escritura streaming ───────────────────────────────────────────────

    def _write_stream(
        self,
        df:           DataFrame,
        contract:     IngestionContract,
        dst_contract: TableContract,
    ) -> int:
        checkpoint = self._resolve_checkpoint(contract)
        self.log.info(
            f"[{contract.name}] streaming write | "
            f"table={dst_contract.full_name} | checkpoint={checkpoint}"
        )

        writer = (
            df.writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", "true")
        )

        if dst_contract.partition_columns:
            writer = writer.partitionBy(*dst_contract.partition_columns)

        if contract.trigger == StreamTrigger.AVAILABLE_NOW:
            writer = writer.trigger(availableNow=True)

        if self._env._is_databricks:
            query = writer.toTable(dst_contract.full_name)
        else:
            delta_path = dst_contract.location or self._local_delta_path(dst_contract)
            Path(delta_path).mkdir(parents=True, exist_ok=True)
            query = writer.option("path", delta_path).start()

        query.awaitTermination()

        # Registrar tabla local si no existe aún
        if not self._env._is_databricks:
            self._register_local_table(dst_contract)

        try:
            return self._spark.read.format("delta").load(
                dst_contract.location or self._local_delta_path(dst_contract)
            ).count()
        except Exception:
            return -1

    def _resolve_checkpoint(self, contract: IngestionContract) -> str:
        """Construye la ruta del checkpoint según entorno."""
        if self._env.has_path("checkpoint"):
            base = self._env.get_path("checkpoint")
        else:
            warehouse = self._spark.conf.get(
                "spark.sql.warehouse.dir", "/tmp/spark-warehouse"
            )
            base = os.path.join(warehouse, "_checkpoints")

        return os.path.join(base, contract.checkpoint_suffix)

    def _local_delta_path(self, dst_contract: TableContract) -> str:
        warehouse = self._spark.conf.get(
            "spark.sql.warehouse.dir", "/tmp/spark-warehouse"
        )
        return os.path.join(
            warehouse, dst_contract.schema, dst_contract.name
        )

    def _register_local_table(self, dst_contract: TableContract) -> None:
        """Registra la tabla en el catálogo local de Spark si no existe."""
        delta_path = dst_contract.location or self._local_delta_path(dst_contract)
        table_ref  = f"{dst_contract.schema}.{dst_contract.name}"
        try:
            self._spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_ref} "
                f"USING DELTA LOCATION '{delta_path}'"
            )
        except Exception as exc:
            self.log.debug(f"Tabla ya registrada o error menor: {exc}")
