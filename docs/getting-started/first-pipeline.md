# Tu primer pipeline

En esta página armamos un proyecto que lleva ventas desde archivos JSON hasta una tabla
de KPIs en Gold.

## 1. Estructura del proyecto

```
mi-pipeline/
├── config/
│   └── config.json
├── ingestion/
│   ├── batch/                  contratos de Landing a Bronze
│   │   └── ventas.json
│   ├── streaming/              contratos de Landing a Bronze en streaming
│   │   └── eventos.json
│   └── silver/                 contratos de Bronze a Silver
│       └── ventas_current.json
├── tables/
│   ├── bronze/
│   │   └── ventas_raw.json
│   ├── silver/
│   │   └── ventas_current.json
│   └── gold/
│       └── kpis_ventas.json
└── pipeline.py
```

Hay dos carpetas de contratos y cumplen papeles distintos. En `tables/` describes cómo
es cada tabla; en `ingestion/` describes cómo llegan los datos a ella. La página
[Contratos](../concepts/contracts.md) explica la diferencia.

## 2. El código del pipeline

```python title="pipeline.py"
from DKOps.launcher import Launcher
from DKOps.ingestion.engine import IngestionEngine
from DKOps.table_governance import load_contract, TableWriter

# Crea la SparkSession y detecta si estamos en local o en Databricks
launcher = Launcher("config/config.json")

engine = IngestionEngine.from_spark(
    spark                   = launcher.spark,
    env                     = launcher.env,
    bronze_contracts_dir    = "ingestion/batch",
    streaming_contracts_dir = "ingestion/streaming",
    silver_contracts_dir    = "ingestion/silver",
    tables_base_dir         = ".",
    ops_path                = "/tmp/mi-pipeline/ops/control",
)

engine.ingest_bronze()     # Landing a Bronze, batch
engine.run_streaming()     # Landing a Bronze, streaming con availableNow
engine.promote_silver()    # Bronze a Silver según la estrategia de cada contrato

# Silver a Gold con SQL y un writer gobernado
ct_silver = load_contract("tables/silver/ventas_current.json")
ct_gold   = load_contract("tables/gold/kpis_ventas.json")

df_kpis = launcher.spark.sql(f"""
    SELECT canal, COUNT(*) AS total_ventas, SUM(precio_total) AS revenue
    FROM {ct_silver.effective_name}
    WHERE is_deleted IS NULL OR NOT is_deleted
    GROUP BY canal
""")

TableWriter(ct_gold).overwrite(df_kpis)

engine.status()
```

Fíjate en `effective_name`: devuelve el nombre de la tabla adecuado para el runtime
actual, así la consulta no depende de si hay catálogo o no.

## 3. Ejecútalo

```bash
python pipeline.py
```

La primera vez en local tarda unos 30 segundos más porque Spark descarga los JAR de
Delta. Después arranca de inmediato.

El pipeline es idempotente: puedes lanzarlo varias veces y no duplicará datos. Bronze
sobrescribe la partición del día, Silver hace upsert y el streaming guarda checkpoints.

## 4. Solo el módulo de gobierno

Si ya tienes datos en Silver y no necesitas la ingesta, puedes usar directamente los
writers y readers:

```python
from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter, TableReader

Launcher("config/config.json")
contract = load_contract("tables/silver/ventas_current.json")

TableWriter(contract).upsert(df, keys=["venta_id"])
df = TableReader(contract).read(filter="estado = 'activo'")
```

## Siguientes pasos

- Entiende cómo encajan las piezas en [Conceptos](../concepts/index.md).
- Revisa las opciones de carga en [Ingesta](../ingestion/index.md).
- Mira un proyecto real en los [demos](../demos/index.md).
