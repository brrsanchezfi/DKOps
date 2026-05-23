"""
pipeline.py — Demo 3: E-commerce — Landing -> Bronze -> Silver -> Gold

Flujo Lakehouse completo para dominio de e-commerce.

Fases:
  0 — Genera datos en Landing (clientes JSON, pedidos CDC JSON, eventos streaming)
  1 — Inicializa DKOps
  2 — Landing -> Bronze (batch: clientes + pedidos; streaming: eventos)
  3 — Bronze -> Silver (full_merge, cdc_merge, append_dedup)
  4 — Silver -> Gold (ventas_canal, clientes_activos)
  5 — Validacion final

Caracteristicas especiales:
  - merge_schema: true en pedidos_raw — schema evolution visible (v1 -> v2)
  - Columnas con mask declarado en clientes y pedidos (email)
  - append_dedup para eventos de clickstream
  - Streaming con availableNow trigger

Uso
---
    cd BigDataFrameworkSpark
    python demos/demo_3/pipeline.py
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
LANDING     = "/tmp/dkops_demo3/landing"
OPS_PATH    = "/tmp/dkops_demo3/ops/control"


def print_header(title: str) -> None:
    print(f"\n{'='*72}")
    print(f"  {title}")
    print('='*72)


def print_phase(n: int, title: str) -> None:
    print(f"\n[{n}] {title}")
    print("-"*60)


def main() -> None:
    print_header("DKOps Demo 3 — E-commerce: Landing -> Bronze -> Silver -> Gold")

    # ---- Fase 0: Generar datos en Landing ----------------------------------
    print_phase(0, "Simulando llegada de datos al Landing...")
    from datagen.main import run as datagen_run
    datagen_run(landing_path=LANDING, n_batches=2, n_clientes=150, n_pedidos=200, n_eventos=100)

    # ---- Fase 1: Inicializar Spark -----------------------------------------
    print_phase(1, "Inicializando DKOps Launcher...")
    from DKOps.launcher import Launcher
    launcher = Launcher(str(CONFIG_PATH))
    spark    = launcher.spark
    env      = launcher.env
    print(f"  Runtime: {env.env} | is_databricks={env.is_databricks}")

    # ---- Fase 2: Landing -> Bronze -----------------------------------------
    print_phase(2, "Ingesta Landing -> Bronze...")
    from DKOps.ingestion.engine import IngestionEngine

    engine = IngestionEngine.from_spark(
        spark                   = spark,
        env                     = env,
        bronze_contracts_dir    = str(DEMO_DIR / "ingestion" / "batch"),
        streaming_contracts_dir = str(DEMO_DIR / "ingestion" / "streaming"),
        silver_contracts_dir    = str(DEMO_DIR / "ingestion" / "silver"),
        tables_base_dir         = str(DEMO_DIR),
        ops_path                = OPS_PATH,
    )

    # Batch: clientes + pedidos
    failed_batch = engine.ingest_bronze()
    if failed_batch:
        print(f"  WARN Fallidos batch: {failed_batch}")
    else:
        print("  OK Ingesta batch completada (clientes, pedidos)")

    # Streaming: eventos web
    print("\n  Ejecutando streaming con availableNow (eventos_web)...")
    engine.run_streaming(timeout=120)
    print("  OK Streaming completado")

    # ---- Fase 3: Bronze -> Silver ------------------------------------------
    print_phase(3, "Promocion Bronze -> Silver...")
    print("  Estrategias: full_merge (clientes), cdc_merge (pedidos), append_dedup (eventos)")

    failed_silver = engine.promote_silver()
    if failed_silver:
        print(f"  WARN Fallidos Silver: {failed_silver}")
    else:
        print("  OK Promocion Silver completada")

    # Demostrar merge_schema en pedidos (schema evolution v1 -> v2)
    from DKOps.table_governance import load_contract
    ct_pedidos_raw = load_contract(str(DEMO_DIR / "tables" / "bronze" / "pedidos_raw.json"))
    pedidos_raw_name = ct_pedidos_raw.effective_name
    schema_cols = spark.read.table(pedidos_raw_name).columns
    v2_cols = {"metodo_envio", "dias_entrega", "calificacion"}
    found_v2 = v2_cols.intersection(set(schema_cols))
    print(f"\n  merge_schema demo: columnas v2 en pedidos_raw: {sorted(found_v2)}")
    if found_v2:
        print("  OK merge_schema activo — columnas nuevas detectadas")

    # Demostrar mask declarado en contratos
    ct_clientes_silver = load_contract(str(DEMO_DIR / "tables" / "silver" / "clientes_current.json"))
    masked_cols = [c.name for c in ct_clientes_silver.masked_columns]
    print(f"\n  Columnas con mask en clientes_current: {masked_cols}")
    if not env._is_databricks:
        print("  (En local: ALTER TABLE SET MASK omitido — se ejecutaria en Databricks)")

    # ---- Fase 4: Silver -> Gold --------------------------------------------
    print_phase(4, "Agregaciones Silver -> Gold...")
    from DKOps.table_governance import TableWriter

    ct_gold_ventas_canal   = load_contract(str(DEMO_DIR / "tables" / "gold" / "ventas_canal.json"))
    ct_gold_clientes_activos = load_contract(str(DEMO_DIR / "tables" / "gold" / "clientes_activos.json"))

    ct_s_pedidos  = load_contract(str(DEMO_DIR / "tables" / "silver" / "pedidos_current.json"))
    ct_s_clientes = load_contract(str(DEMO_DIR / "tables" / "silver" / "clientes_current.json"))
    ct_s_eventos  = load_contract(str(DEMO_DIR / "tables" / "silver" / "eventos_current.json"))

    pedidos_table  = ct_s_pedidos.effective_name
    clientes_table = ct_s_clientes.effective_name
    eventos_table  = ct_s_eventos.effective_name

    # Gold: ventas por canal
    print("  Calculando gold_ventas_canal...")
    df_ventas_canal = spark.sql(f"""
        SELECT
            canal,
            COUNT(*)                                                         AS total_pedidos,
            SUM(CASE WHEN is_deleted IS NULL OR NOT is_deleted THEN 1 ELSE 0 END) AS pedidos_activos,
            ROUND(SUM(COALESCE(total_usd, 0)), 2)                            AS monto_total_usd,
            ROUND(AVG(total_usd), 2)                                         AS ticket_prom_usd,
            ROUND(
                SUM(CASE WHEN estado = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            )                                                                AS pct_cancelados
        FROM {pedidos_table}
        WHERE canal IS NOT NULL
        GROUP BY canal
        ORDER BY monto_total_usd DESC
    """)
    TableWriter(ct_gold_ventas_canal).overwrite(df_ventas_canal)
    print(f"    Filas gold_ventas_canal: {df_ventas_canal.count()}")

    # Gold: clientes activos por pais y segmento
    print("  Calculando gold_clientes_activos...")
    df_clientes_activos = spark.sql(f"""
        SELECT
            c.pais,
            c.segmento,
            COUNT(DISTINCT c.cliente_id)                                     AS total_clientes,
            SUM(CASE WHEN c.activo = true THEN 1 ELSE 0 END)                 AS clientes_activos,
            ROUND(SUM(CASE WHEN c.activo = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                                             AS pct_activos,
            COUNT(e.evento_id)                                               AS total_eventos,
            ROUND(AVG(e.duracion_seg), 2)                                    AS duracion_prom_seg
        FROM {clientes_table} c
        LEFT JOIN {eventos_table}  e ON c.cliente_id = e.cliente_id
        WHERE c.pais IS NOT NULL AND c.segmento IS NOT NULL
        GROUP BY c.pais, c.segmento
        ORDER BY clientes_activos DESC
    """)
    TableWriter(ct_gold_clientes_activos).overwrite(df_clientes_activos)
    print(f"    Filas gold_clientes_activos: {df_clientes_activos.count()}")

    # ---- Fase 5: Validacion -----------------------------------------------
    print_phase(5, "Validacion final...")
    engine.status()

    gold_ventas_name   = ct_gold_ventas_canal.effective_name
    gold_clientes_name = ct_gold_clientes_activos.effective_name

    print("\n  -- Ventas por canal --")
    spark.sql(f"""
        SELECT canal, total_pedidos, monto_total_usd, ticket_prom_usd, pct_cancelados
        FROM {gold_ventas_name}
        ORDER BY monto_total_usd DESC
    """).show(truncate=False)

    print("\n  -- Clientes activos por pais (top 5) --")
    spark.sql(f"""
        SELECT pais, SUM(clientes_activos) AS activos, SUM(total_clientes) AS total
        FROM {gold_clientes_name}
        GROUP BY pais
        ORDER BY activos DESC
        LIMIT 5
    """).show(truncate=False)

    n_clientes = spark.read.table(clientes_table).count()
    n_pedidos  = spark.read.table(pedidos_table).count()
    n_eventos  = spark.read.table(eventos_table).count()

    print(f"""
  +--------------------------------------------------+
  |     RESUMEN LAKEHOUSE — Demo 3: E-commerce       |
  +--------------------------------------------------+
  |  Silver clientes_current   : {n_clientes:>6}                 |
  |  Silver pedidos_current    : {n_pedidos:>6}                 |
  |  Silver eventos_current    : {n_eventos:>6}                 |
  |  Gold ventas_canal         : {df_ventas_canal.count():>6}                 |
  |  Gold clientes_activos     : {df_clientes_activos.count():>6}                 |
  |  merge_schema activo       : SI (pedidos v1+v2)              |
  |  Column masks declarados   : email (clientes, pedidos)       |
  +--------------------------------------------------+
    """)

    print("\n" + "="*72)
    print("  Demo 3 completado.")
    print("="*72 + "\n")


if __name__ == "__main__":
    main()
