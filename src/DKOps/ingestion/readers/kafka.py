"""
kafka.py — Lectura streaming desde Confluent Kafka / Apache Kafka.

Requiere el conector kafka disponible en el classpath de Spark.
En Databricks, se incluye por defecto. En local, agrega:
  spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:<version>

Configuración en el IngestionContract (source.kafka):
    {
      "topic": "farmia.sensors",
      "starting_offsets": "earliest",
      "bootstrap_servers": "broker:9092",
      "sasl_username": "...",
      "sasl_password": "..."
    }

Para producción, los credentials deben venir de Databricks Secrets o .env local.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from DKOps.ingestion.contracts.ingestion_contract import IngestionContract
from DKOps.ingestion.readers.base import BaseSourceReader
from DKOps.ingestion.readers._schema_helper import build_spark_schema


class KafkaReader(BaseSourceReader):
    """
    Streaming reader desde Kafka. Devuelve DataFrame con las columnas
    nativas de Kafka: key, value, topic, partition, offset, timestamp.

    El MetadataEnricher con add_kafka_metadata=True añade columnas técnicas
    (_kafka_topic, _kafka_offset, _raw_value, etc.) y el BronzeIngestor
    parsea _raw_value según el schema del contrato.
    """

    def __init__(
        self,
        contract:    IngestionContract,
        spark:       SparkSession,
        kafka_creds: dict | None = None,
    ) -> None:
        super().__init__(contract)
        self._spark      = spark
        self._kafka_creds = kafka_creds or {}

    def read(self) -> DataFrame:
        src        = self.contract.source
        kafka_cfg  = src.kafka
        topic      = kafka_cfg.get("topic", "")
        servers    = kafka_cfg.get("bootstrap_servers", self._kafka_creds.get("bootstrap.servers", ""))
        start_off  = kafka_cfg.get("starting_offsets", "earliest")

        self.log.info(
            f"[{self.contract.name}] KafkaReader | "
            f"topic={topic} | servers={servers}"
        )

        reader = (
            self._spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", servers)
                .option("subscribe",               topic)
                .option("startingOffsets",         start_off)
                .option("failOnDataLoss",           "false")
        )

        # SASL/SSL si hay credenciales
        username = self._kafka_creds.get("sasl.username", "")
        password = self._kafka_creds.get("sasl.password", "")
        if username and password:
            jaas = (
                f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                f"required username='{username}' password='{password}';"
            )
            reader = (
                reader
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism",    "PLAIN")
                .option("kafka.sasl.jaas.config",  jaas)
            )

        df = reader.load()

        # Parsear value bytes → JSON string si hay schema
        if src.schema:
            spark_schema = build_spark_schema(list(src.schema))
            df = (
                df.withColumn("_raw_value", F.col("value").cast(StringType()))
                  .withColumn("_parsed",    F.from_json(F.col("_raw_value"), spark_schema))
                  .select(
                      F.col("key"), F.col("topic"), F.col("partition"),
                      F.col("offset"), F.col("timestamp"), F.col("_raw_value"),
                      *[F.col(f"_parsed.{f['name']}") for f in src.schema],
                  )
            )

        return df
