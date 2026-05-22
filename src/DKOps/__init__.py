"""
DKOps — Framework de gobernanza e ingesta para Data Lakehouses Delta.

Módulos
-------
table_governance  : gobernanza de tablas (writers, readers, contracts, migrations)
ingestion         : motor de ingesta Landing → Bronze → Silver

Uso rápido — Gobernanza:

    from DKOps.launcher import Launcher
    from DKOps.table_governance.contracts.loader import load_contract
    from DKOps.table_governance.writers.table_writer import TableWriter

    launcher = Launcher("config.json")
    contract = load_contract("tables/bronze/ventas_raw.json")
    writer   = TableWriter(contract)
    writer.append(df)

Uso rápido — Ingesta:

    from DKOps.launcher import Launcher
    from DKOps.ingestion import IngestionEngine

    launcher = Launcher("config.json")
    engine   = IngestionEngine.from_launcher(
        bronze_contracts_dir = "ingestion/batch",
        silver_contracts_dir = "ingestion/silver",
        tables_base_dir      = ".",
        ops_path             = "/tmp/ops/control",
    )
    engine.ingest_bronze()
    engine.promote_silver()

Nota sobre imports
------------------
Los componentes con dependencia de PySpark (writers, readers, engine)
se importan desde sus submódulos directamente para permitir importar
los contratos y tipos puros sin PySpark instalado.
"""
