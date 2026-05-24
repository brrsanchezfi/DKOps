"""
pipeline.py — Demo 1: Aeronautica — Landing -> Bronze -> Silver -> Gold

Ejecuta de forma secuencial el flujo Lakehouse completo:
  Fase 0: Genera datos en Landing (vuelos diarios JSON, aeropuertos CSV)
  Fase 1: Inicializa DKOps (Launcher + SparkSession)
  Fase 2: Landing -> Bronze (IngestionEngine batch)
  Fase 3: Bronze -> Silver (full_merge para vuelos y aeropuertos)
  Fase 4: Silver -> Gold (KPIs de puntualidad y top rutas)
  Fase 5: Validacion final + SafeMigrator demo

Caracteristicas especiales:
  - SafeMigrator: demo de plan de migracion (dry_run)
  - Columna retraso_llegada_min tipada como INTEGER en contrato Bronze

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_1/pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT     = Path(__file__).resolve().parents[2]
SRC      = ROOT / "src"
DEMO_DIR = Path(__file__).resolve().parent

if str(SRC)      not in sys.path: sys.path.insert(0, str(SRC))
if str(DEMO_DIR) not in sys.path: sys.path.insert(0, str(DEMO_DIR))

CONFIG_PATH = DEMO_DIR / "config" / "config.json"
LANDING     = "/tmp/dkops_demo1/landing"
OPS_PATH    = "/tmp/dkops_demo1/ops/control"


def print_header(title: str) -> None:
    print(f"\n{'='*65}")
    print(f"  {title}")
    print('='*65)


def print_phase(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")
    print("-"*55)


def main() -> None:
    print_header("DKOps Demo 1 — Aeronautica: Landing -> Bronze -> Silver -> Gold")

    # ---- Fase 0: Generar datos en Landing -----------------------------------
    print_phase(0, "Simulando llegada de datos al Landing...")
    from datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2, n_vuelos=200)

    # ---- Fase 1: Inicializar Spark ------------------------------------------
    print_phase(1, "Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ---- Fase 2: Landing -> Bronze -----------------------------------------
    print_phase(2, "Ingesta Landing -> Bronze (IngestionEngine batch)...")
    from DKOps.ingestion.engine import IngestionEngine

    engine = IngestionEngine.from_spark(
        spark                = spark,
        env                  = env,
        bronze_contracts_dir = str(DEMO_DIR / "ingestion" / "batch"),
        silver_contracts_dir = str(DEMO_DIR / "ingestion" / "silver"),
        tables_base_dir      = str(DEMO_DIR),
        ops_path             = OPS_PATH,
    )
    print(f"  Bronze contracts: {len(engine._bronze_contracts)}")
    print(f"  Silver contracts: {len(engine._silver_contracts)}")

    failed_bronze = engine.ingest_bronze()
    if failed_bronze:
        print(f"  WARN Fallidos Bronze: {failed_bronze}")
    else:
        print("  OK Ingesta Bronze completada")

    # ---- Fase 3: Bronze -> Silver ------------------------------------------
    print_phase(3, "Promocion Bronze -> Silver (full_merge)...")
    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  WARN Fallidos Silver: {failed_silver}")
    else:
        print("  OK Promocion Silver completada")

    # ---- Fase 4: Silver -> Gold --------------------------------------------
    print_phase(4, "Agregaciones Silver -> Gold (KPIs)...")
    from DKOps.table_governance import load_contract, TableWriter, SafeMigrator

    ct_gold_puntualidad = load_contract(str(DEMO_DIR / "tables" / "gold" / "puntualidad.json"))
    ct_gold_rutas       = load_contract(str(DEMO_DIR / "tables" / "gold" / "rutas_top.json"))

    ct_silver_vuelos = load_contract(str(DEMO_DIR / "tables" / "silver" / "vuelos_current.json"))
    vuelos_table = ct_silver_vuelos.effective_name

    # KPI puntualidad por aerolinea
    print("  Calculando gold_puntualidad...")
    df_puntualidad = spark.sql(f"""
        SELECT
            iata_aerolinea,
            COUNT(*)                                                  AS total_vuelos,
            SUM(CASE WHEN estado = 'ON_TIME'    THEN 1 ELSE 0 END)   AS vuelos_a_tiempo,
            SUM(CASE WHEN estado = 'DELAYED'    THEN 1 ELSE 0 END)   AS vuelos_demorados,
            SUM(CASE WHEN estado = 'CANCELLED'  THEN 1 ELSE 0 END)   AS vuelos_cancelados,
            ROUND(
                SUM(CASE WHEN estado = 'ON_TIME' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            )                                                         AS pct_a_tiempo,
            ROUND(AVG(retraso_llegada_min), 2)                        AS retraso_prom_min,
            ROUND(AVG(pasajeros * 100.0 / NULLIF(capacidad, 0)), 2)   AS ocupacion_prom_pct
        FROM {vuelos_table}
        WHERE iata_aerolinea IS NOT NULL
        GROUP BY iata_aerolinea
        ORDER BY pct_a_tiempo DESC
    """)
    TableWriter(ct_gold_puntualidad).overwrite(df_puntualidad)
    print(f"    Filas gold_puntualidad: {df_puntualidad.count()}")

    # KPI top rutas por volumen
    print("  Calculando gold_rutas_top...")
    df_rutas = spark.sql(f"""
        SELECT
            iata_origen,
            iata_destino,
            COUNT(*)                                                AS total_vuelos,
            SUM(COALESCE(pasajeros, 0))                             AS total_pasajeros,
            FIRST(distancia_km)                                     AS distancia_km,
            ROUND(AVG(retraso_llegada_min), 2)                      AS retraso_prom_min,
            ROUND(AVG(pasajeros * 100.0 / NULLIF(capacidad, 0)), 2) AS ocupacion_prom_pct
        FROM {vuelos_table}
        WHERE iata_origen IS NOT NULL AND iata_destino IS NOT NULL
        GROUP BY iata_origen, iata_destino
        ORDER BY total_vuelos DESC
        LIMIT 30
    """)
    TableWriter(ct_gold_rutas).overwrite(df_rutas)
    print(f"    Filas gold_rutas_top: {df_rutas.count()}")

    # ---- Fase 5: Validacion + SafeMigrator ---------------------------------
    print_phase(5, "Validacion final + SafeMigrator demo...")

    engine.status()

    # SafeMigrator dry_run sobre Bronze
    ct_bronze_vuelos = load_contract(str(DEMO_DIR / "tables" / "bronze" / "vuelos_raw.json"))
    print("\n  SafeMigrator dry_run en vuelos_raw:")
    SafeMigrator(ct_bronze_vuelos, dry_run=True).apply()

    # Consultas Gold
    gold_puntualidad_name = ct_gold_puntualidad.effective_name
    gold_rutas_name       = ct_gold_rutas.effective_name

    print("\n  -- Top 5 aerolineas por puntualidad --")
    spark.sql(f"""
        SELECT iata_aerolinea, total_vuelos, pct_a_tiempo, retraso_prom_min
        FROM {gold_puntualidad_name}
        ORDER BY pct_a_tiempo DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n  -- Top 10 rutas por volumen --")
    spark.sql(f"""
        SELECT iata_origen, iata_destino, total_vuelos, total_pasajeros, retraso_prom_min
        FROM {gold_rutas_name}
        ORDER BY total_vuelos DESC
        LIMIT 10
    """).show(truncate=False)

    # Resumen ejecutivo
    try:
        ct_silver_aeropuertos = load_contract(
            str(DEMO_DIR / "tables" / "silver" / "aeropuertos_current.json")
        )
        n_aeropuertos = spark.read.table(ct_silver_aeropuertos.effective_name).count()
    except Exception:
        n_aeropuertos = 0

    n_vuelos_silver = spark.read.table(vuelos_table).count()

    print(f"""
  +--------------------------------------------------+
  |     RESUMEN LAKEHOUSE — Demo 1: Aeronautica      |
  +--------------------------------------------------+
  |  Aeropuertos en Silver   : {n_aeropuertos:>6}                 |
  |  Vuelos en Silver        : {n_vuelos_silver:>6}                 |
  |  Filas gold_puntualidad  : {df_puntualidad.count():>6}                 |
  |  Filas gold_rutas_top    : {df_rutas.count():>6}                 |
  +--------------------------------------------------+
    """)

    print("\n" + "="*65)
    print("  Demo 1 completado.")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()
