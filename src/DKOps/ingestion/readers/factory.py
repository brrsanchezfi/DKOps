"""
factory.py — SourceReaderFactory: selecciona el reader correcto por entorno.

Matriz de compatibilidad:

  Entorno         | Batch                  | Streaming
  ----------------|------------------------|---------------------------
  Local PC        | LocalBatchReader       | FileStreamReader
  Databricks Conn | LocalBatchReader       | FileStreamReader / Kafka
  Databricks WS   | AutoLoaderReader       | AutoLoaderReader / Kafka

Reglas de selección:
  1. Si source.format == "kafka"   → KafkaReader (siempre)
  2. Si streaming + no Kafka       → AutoLoaderReader (Databricks) | FileStreamReader (local)
  3. Si batch                      → AutoLoaderReader (Databricks) | LocalBatchReader (local)

Los readers de Databricks nunca se instancian en local — el factory los guarda
bajo importación diferida para que el módulo sea importable sin Spark/Databricks.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from DKOps.environment_config import EnvironmentConfig
from DKOps.ingestion.contracts.ingestion_contract import IngestionContract, IngestionType
from DKOps.ingestion.readers.base import BaseSourceReader


class SourceReaderFactory:
    """Fábrica de readers. Todos los métodos son estáticos."""

    @staticmethod
    def create(
        contract:    IngestionContract,
        spark:       SparkSession,
        env:         EnvironmentConfig,
        kafka_creds: dict | None         = None,
        schema_root: str | None          = None,
    ) -> BaseSourceReader:
        """
        Instancia el reader correcto según el contrato y el entorno activo.

        Parámetros
        ----------
        contract    : IngestionContract a procesar.
        spark       : SparkSession activa.
        env         : EnvironmentConfig del Launcher.
        kafka_creds : credenciales Kafka (solo para KafkaReader).
        schema_root : ruta base para los schemas de Auto Loader (solo Databricks).
        """
        is_databricks = env._is_databricks
        fmt           = contract.source.format.lower()
        is_streaming  = contract.is_streaming()

        # Kafka — siempre KafkaReader independiente del entorno
        if fmt == "kafka":
            from DKOps.ingestion.readers.kafka import KafkaReader
            return KafkaReader(contract, spark, kafka_creds)

        # Streaming de archivos
        if is_streaming:
            if is_databricks:
                # Auto Loader en modo streaming
                schema_loc = f"{schema_root}/{contract.name}" if schema_root else f"/tmp/schemas/{contract.name}"
                from DKOps.ingestion.readers.autoloader import AutoLoaderReader
                return AutoLoaderReader(contract, spark, schema_location=schema_loc)
            # Local: file streaming estándar de Spark
            from DKOps.ingestion.readers.file_stream import FileStreamReader
            return FileStreamReader(contract, spark)

        # Batch
        if is_databricks and fmt != "delta":
            # Auto Loader con trigger=availableNow — se comporta como batch
            schema_loc = f"{schema_root}/{contract.name}" if schema_root else f"/tmp/schemas/{contract.name}"
            from DKOps.ingestion.readers.autoloader import AutoLoaderReader
            return AutoLoaderReader(contract, spark, schema_location=schema_loc)

        # Local batch (o delta source, que no pasa por Auto Loader)
        from DKOps.ingestion.readers.local_batch import LocalBatchReader
        return LocalBatchReader(contract, spark)
