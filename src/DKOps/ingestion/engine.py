"""
engine.py — IngestionEngine: fachada principal del módulo de ingesta.

Punto de entrada único para operaciones de ingesta. Carga contratos,
instancia ingestores y expone una API simple:

    engine = IngestionEngine.from_launcher(
        bronze_contracts_dir = "demos/demo_5/ingestion/batch",
        silver_contracts_dir = "demos/demo_5/ingestion/silver",
        tables_base_dir      = "demos/demo_5",
        ops_path             = "/tmp/ops/demo5/control",
    )

    # Landing → Bronze (batch)
    engine.ingest_bronze()                    # todos los datasets
    engine.ingest_bronze("ventas_diarias")    # uno específico

    # Bronze → Silver
    engine.promote_silver()
    engine.promote_silver("ventas_current")

    # Streaming (Landing → Bronze)
    engine.run_streaming()                    # availableNow: procesa y para
    queries = engine.start_streaming()        # continuo: devuelve queries activas
    engine.stop_streaming(queries)

    # Observabilidad
    engine.status()                           # conteo de filas por tabla
    engine.ops.read().show()                  # tabla de control operativo

Desacoplamiento:
  Las lakehouse applications (demos, pipelines de negocio) son INDEPENDIENTES
  del IngestionEngine. Usan TableReader/TableWriter del módulo governance
  directamente. El IngestionEngine solo mueve datos crudos hasta Silver.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

from DKOps.environment_config import EnvironmentConfig
from DKOps.ingestion.bronze_ingestor import BronzeIngestor
from DKOps.ingestion.contracts.ingestion_contract import IngestionContract, IngestionType
from DKOps.ingestion.contracts.loader import IngestionContractLoader
from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
from DKOps.ingestion.silver_promoter import SilverPromoter
from DKOps.logger_config import LoggableMixin
from DKOps.table_governance.contracts.loader import ContractLoader, TableContract


class IngestionEngine(LoggableMixin):
    """
    Orquestador principal del módulo de ingesta.
    No instanciar directamente — usa from_launcher() o from_spark().
    """

    def __init__(
        self,
        spark:           SparkSession,
        env:             EnvironmentConfig,
        bronze_contracts: list[IngestionContract],
        silver_contracts: list[IngestionContract],
        bronze_tables:   dict[str, TableContract],
        silver_tables:   dict[str, TableContract],
        ops:             IngestionOpsLogger | None,
        schema_root:     str | None = None,
        kafka_creds:     dict | None = None,
    ) -> None:
        self._spark            = spark
        self._env              = env
        self._bronze_contracts = bronze_contracts
        self._silver_contracts = silver_contracts
        self._bronze_tables    = bronze_tables
        self._silver_tables    = silver_tables
        self.ops               = ops

        self._bronze_ingestor = BronzeIngestor(
            spark       = spark,
            env         = env,
            ops         = ops,
            schema_root = schema_root,
            kafka_creds = kafka_creds,
        )
        self._silver_promoter = SilverPromoter(spark, env, ops)

    # ── Factory methods ───────────────────────────────────────────────────

    @classmethod
    def from_launcher(
        cls,
        bronze_contracts_dir: str | None = None,
        silver_contracts_dir: str | None = None,
        streaming_contracts_dir: str | None = None,
        tables_base_dir:      str         = ".",
        ops_path:             str | None  = None,
        schema_root:          str | None  = None,
        kafka_creds:          dict | None = None,
    ) -> "IngestionEngine":
        """
        Crea el engine desde el Launcher activo.

        Parámetros
        ----------
        bronze_contracts_dir    : dir con JSONs de ingesta batch Landing→Bronze
        silver_contracts_dir    : dir con JSONs de promoción Bronze→Silver
        streaming_contracts_dir : dir con JSONs de ingesta streaming
        tables_base_dir         : dir base para resolver destination_contract paths
        ops_path                : path Delta para tabla de control operativo
        schema_root             : path para schemas de Auto Loader (Databricks)
        kafka_creds             : credenciales Kafka
        """
        from DKOps.launcher import Launcher
        launcher = Launcher.current()
        return cls.from_spark(
            spark                   = launcher.spark,
            env                     = launcher.env,
            bronze_contracts_dir    = bronze_contracts_dir,
            silver_contracts_dir    = silver_contracts_dir,
            streaming_contracts_dir = streaming_contracts_dir,
            tables_base_dir         = tables_base_dir,
            ops_path                = ops_path,
            schema_root             = schema_root,
            kafka_creds             = kafka_creds,
        )

    @classmethod
    def from_spark(
        cls,
        spark:                   SparkSession,
        env:                     EnvironmentConfig,
        bronze_contracts_dir:    str | None = None,
        silver_contracts_dir:    str | None = None,
        streaming_contracts_dir: str | None = None,
        tables_base_dir:         str        = ".",
        ops_path:                str | None = None,
        schema_root:             str | None = None,
        kafka_creds:             dict | None = None,
    ) -> "IngestionEngine":
        """Crea el engine desde SparkSession y EnvironmentConfig explícitos."""
        table_loader     = ContractLoader(env)
        ingestion_loader = IngestionContractLoader(
            contracts_dir = bronze_contracts_dir or tables_base_dir,
            base_dir      = tables_base_dir,
            env           = env,
        )

        # Cargar contratos de ingesta batch
        bronze_contracts: list[IngestionContract] = []
        if bronze_contracts_dir and Path(bronze_contracts_dir).is_dir():
            bronze_contracts = [
                c for c in ingestion_loader.load_all()
                if c.ingest_type == IngestionType.BATCH and not c.is_silver_promotion()
            ]

        # Cargar contratos streaming
        streaming_contracts: list[IngestionContract] = []
        if streaming_contracts_dir and Path(streaming_contracts_dir).is_dir():
            stream_loader = IngestionContractLoader(
                contracts_dir = streaming_contracts_dir,
                base_dir      = tables_base_dir,
                env           = env,
            )
            streaming_contracts = stream_loader.load_all()

        bronze_contracts = bronze_contracts + streaming_contracts

        # Cargar contratos Silver
        silver_contracts: list[IngestionContract] = []
        if silver_contracts_dir and Path(silver_contracts_dir).is_dir():
            silver_loader = IngestionContractLoader(
                contracts_dir = silver_contracts_dir,
                base_dir      = tables_base_dir,
                env           = env,
            )
            silver_contracts = silver_loader.load_all()

        # Cargar TableContracts para cada ingestion contract
        bronze_tables = cls._load_table_contracts(
            bronze_contracts, tables_base_dir, ingestion_loader, table_loader
        )
        silver_tables, silver_src_tables = cls._load_silver_table_contracts(
            silver_contracts, tables_base_dir, ingestion_loader, table_loader
        )

        # OpsLogger
        ops = None
        if ops_path:
            ops = IngestionOpsLogger(spark, ops_path)

        return cls(
            spark            = spark,
            env              = env,
            bronze_contracts = bronze_contracts,
            silver_contracts = silver_contracts,
            bronze_tables    = bronze_tables,
            silver_tables    = {**bronze_tables, **silver_tables},
            ops              = ops,
            schema_root      = schema_root,
            kafka_creds      = kafka_creds,
        )

    # ── API pública: batch ────────────────────────────────────────────────

    def ingest_bronze(self, name: str | None = None) -> list[str]:
        """
        Ingesta batch Landing → Bronze.

        Parámetros
        ----------
        name : nombre del dataset. Si None, ejecuta todos los batch (no streaming).

        Devuelve
        --------
        Lista de datasets que fallaron.
        """
        batch_contracts = [
            c for c in self._bronze_contracts
            if not c.is_streaming() and not c.is_silver_promotion()
        ]

        if name:
            batch_contracts = [c for c in batch_contracts if c.name == name]
            if not batch_contracts:
                raise ValueError(f"Dataset batch '{name}' no encontrado.")

        return self._bronze_ingestor.ingest_all(batch_contracts, self._bronze_tables)

    def promote_silver(self, name: str | None = None) -> list[str]:
        """
        Promoción Bronze → Silver.

        Parámetros
        ----------
        name : nombre del dataset. Si None, ejecuta todos.
        """
        contracts = self._silver_contracts
        if name:
            contracts = [c for c in contracts if c.name == name]
            if not contracts:
                raise ValueError(f"Dataset Silver '{name}' no encontrado.")

        failed = []
        for c in contracts:
            src = self._bronze_tables.get(c.name) or self._bronze_tables.get(
                Path(c.source_contract_path or "").stem
            )
            dst = self._silver_tables.get(c.name)
            if src is None or dst is None:
                self.log.warning(f"[{c.name}] Faltan contratos src/dst — omitido")
                continue
            try:
                self._silver_promoter.promote(c, src, dst)
            except Exception as exc:
                self.log.error(f"[{c.name}] Silver error: {exc}")
                failed.append(c.name)

        return failed

    # ── API pública: streaming ────────────────────────────────────────────

    def run_streaming(
        self,
        name:    str | None = None,
        timeout: int        = 120,
    ) -> None:
        """
        Ejecuta ingesta streaming con trigger=availableNow.
        Procesa todos los mensajes/archivos pendientes y para.
        Bloquea hasta completar (o hasta timeout por query).
        """
        contracts = [c for c in self._bronze_contracts if c.is_streaming()]
        if name:
            contracts = [c for c in contracts if c.name == name]

        for c in contracts:
            dst = self._bronze_tables.get(c.name)
            if dst is None:
                self.log.warning(f"[{c.name}] Sin TableContract — omitido")
                continue
            self._bronze_ingestor.ingest(c, dst)

    def start_streaming(
        self,
        name: str | None = None,
    ) -> list:
        """
        Arranca queries streaming en modo continuo (no bloquea).
        Devuelve lista de StreamingQuery para monitorizar o parar.

        Útil para dejarlo corriendo en background mientras ejecutas batch.
        """
        contracts = [c for c in self._bronze_contracts if c.is_streaming()]
        if name:
            contracts = [c for c in contracts if c.name == name]

        queries = []
        for c in contracts:
            dst = self._bronze_tables.get(c.name)
            if dst is None:
                continue
            reader = __import__(
                "DKOps.ingestion.readers.factory", fromlist=["SourceReaderFactory"]
            ).SourceReaderFactory.create(c, self._spark, self._env, self._kafka_creds)
            df = reader.read()
            from DKOps.ingestion.enrichment.metadata import MetadataEnricher
            df = MetadataEnricher().enrich(df, c.metadata, c.source.format)

            checkpoint = self._bronze_ingestor._resolve_checkpoint(c)
            writer = (
                df.writeStream
                  .format("delta")
                  .option("checkpointLocation", checkpoint)
                  .option("mergeSchema", "true")
            )
            if dst.partition_columns:
                writer = writer.partitionBy(*dst.partition_columns)

            if self._env._is_databricks:
                query = writer.toTable(dst.full_name)
            else:
                from pathlib import Path
                delta_path = dst.location or self._bronze_ingestor._local_delta_path(dst)
                Path(delta_path).mkdir(parents=True, exist_ok=True)
                query = writer.option("path", delta_path).start()

            queries.append((c.name, query))
            self.log.info(f"[{c.name}] Query streaming arrancada ✔")

        return queries

    def stop_streaming(self, queries: list) -> None:
        """Para todas las StreamingQuery activas devueltas por start_streaming()."""
        for name, query in queries:
            self.log.info(f"Parando query [{name}]")
            query.stop()
            self.log.info(f"[{name}] Parada ✔")

    # ── Observabilidad ────────────────────────────────────────────────────

    def status(self) -> None:
        """Imprime conteo de filas de todas las tablas Bronze y Silver registradas."""
        self.log.info("=== Estado de tablas ===")
        all_tables = {**self._bronze_tables, **self._silver_tables}
        for name, contract in all_tables.items():
            try:
                table_ref = (
                    contract.full_name
                    if self._env._is_databricks
                    else f"{contract.schema}.{contract.name}"
                )
                count = self._spark.table(table_ref).count()
                self.log.info(f"  {table_ref}: {count:,} filas")
            except Exception as exc:
                self.log.warning(f"  {name}: no disponible ({exc})")

    # ── Helpers privados ──────────────────────────────────────────────────

    @staticmethod
    def _load_table_contracts(
        contracts:        list[IngestionContract],
        base_dir:         str,
        ingestion_loader: IngestionContractLoader,
        table_loader:     ContractLoader,
    ) -> dict[str, TableContract]:
        result = {}
        for c in contracts:
            try:
                dst = ingestion_loader.load_destination(c)
                result[c.name] = dst
            except Exception as exc:
                raise RuntimeError(
                    f"Error cargando TableContract destino para '{c.name}': {exc}"
                ) from exc
        return result

    @staticmethod
    def _load_silver_table_contracts(
        contracts:        list[IngestionContract],
        base_dir:         str,
        ingestion_loader: IngestionContractLoader,
        table_loader:     ContractLoader,
    ) -> tuple[dict[str, TableContract], dict[str, TableContract]]:
        dst_contracts = {}
        src_contracts = {}
        for c in contracts:
            try:
                dst = ingestion_loader.load_destination(c)
                dst_contracts[c.name] = dst
                if c.source_contract_path:
                    src = ingestion_loader.load_source(c)
                    if src:
                        src_contracts[c.name] = src
            except Exception as exc:
                raise RuntimeError(
                    f"Error cargando contratos Silver para '{c.name}': {exc}"
                ) from exc
        return dst_contracts, src_contracts
