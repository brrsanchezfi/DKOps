"""
DKOps Ingestion Module
======================
Motor de ingesta Landing → Bronze → Silver integrado con el framework DKOps.

Uso rápido:

    from DKOps.ingestion.engine import IngestionEngine

    engine = IngestionEngine.from_launcher(
        bronze_contracts_dir = "demos/demo_5/ingestion/batch",
        silver_contracts_dir = "demos/demo_5/ingestion/silver",
        tables_base_dir      = "demos/demo_5",
        ops_path             = "/tmp/ops/control",
    )

    engine.ingest_bronze()      # Landing → Bronze (batch)
    engine.run_streaming()      # Landing → Bronze (streaming, availableNow)
    engine.promote_silver()     # Bronze → Silver

Contratos (sin PySpark):

    from DKOps.ingestion.contracts.ingestion_contract import IngestionContract, IngestionType
    from DKOps.ingestion.contracts.loader import IngestionContractLoader

Componentes con PySpark (importar directamente desde submodulo):

    from DKOps.ingestion.engine import IngestionEngine
    from DKOps.ingestion.bronze_ingestor import BronzeIngestor
    from DKOps.ingestion.silver_promoter import SilverPromoter
    from DKOps.ingestion.enrichment.metadata import MetadataEnricher
    from DKOps.ingestion.ops.ops_logger import IngestionOpsLogger
    from DKOps.ingestion.readers.factory import SourceReaderFactory
"""
