"""
pipeline_lectura.py
===================
Demo 4 — TableReader: lectura gobernada por contrato y Change Data Feed.

Demuestra las cuatro capacidades de TableReader:

  FEATURE 1 — read()
    Lectura completa con opciones de filter, columns y limit.
    El resultado es un DataFrame nativo de PySpark — todos los métodos
    de transformación (.filter, .groupBy, .join, etc.) funcionan sin cambios.

  FEATURE 2 — read_partition()
    Lectura eficiente de una partición específica. El lector valida que
    la columna sea realmente de partición antes de consultar.

  FEATURE 3 — read_stream()
    Lectura como Structured Streaming DataFrame usando el log Delta.

  FEATURE 4 — read_cdf()
    Change Data Feed: captura qué filas cambiaron, cómo y en qué versión.
    Requiere "change_data_feed": true en el contrato — el CreateWriter lo
    activa automáticamente en TBLPROPERTIES.

Dominio: tienda con inventario de productos y ventas diarias.

Ejecutar:
    python pipeline_lectura.py
"""

from __future__ import annotations

from datetime import date

from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter, TableReader

from data_generator import DataGenerator


# ─────────────────────────────────────────────────────────────────────────────
# Helpers visuales
# ─────────────────────────────────────────────────────────────────────────────

def _sep(titulo: str) -> None:
    print(f"\n{'═' * 72}")
    print(f"  {titulo}")
    print('═' * 72)


def _sub(titulo: str) -> None:
    print(f"\n  ── {titulo} {'─' * max(1, 60 - len(titulo))}")


# ─────────────────────────────────────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────────────────────────────────────

launcher = Launcher("config/config.json", log_filename="lecturaDelta")
spark    = launcher.spark
gen      = DataGenerator(spark)


# ── Cargar contratos ──────────────────────────────────────────────────────────

_sep("Cargando contratos")

ct_inventario   = load_contract("tables/inventario.json")
ct_ventas       = load_contract("tables/ventas_diarias.json")

print(f"  ✔ {ct_inventario.effective_name:50s}  change_data_feed={ct_inventario.change_data_feed}")
print(f"  ✔ {ct_ventas.effective_name:50s}  change_data_feed={ct_ventas.change_data_feed}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 — Bootstrap: carga inicial del inventario y ventas
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 1 — Bootstrap: carga inicial")

_sub("Inventario inicial (versión 0)")
df_inv = gen.inventario_inicial()
print(f"    Productos generados: {df_inv.count()}")
print(f"    Schema: {df_inv.columns}")
TableWriter(ct_inventario).overwrite(df_inv)
print(f"    ✔ Tabla '{ct_inventario.effective_name}' creada con CDF habilitado")

_sub("Verificando TBLPROPERTIES — enableChangeDataFeed")
spark.sql(f"SHOW TBLPROPERTIES {ct_inventario.effective_name}").show(truncate=False)

_sub("Ventas del día 1 (2024-01-15)")
df_v1 = gen.ventas_fecha(df_inv, date(2024, 1, 15), n=80)
TableWriter(ct_ventas).overwrite_partition(df_v1, partition={"fecha": "2024-01-15"})
print(f"    ✔ {df_v1.count()} ventas del 2024-01-15 escritas")

_sub("Ventas del día 2 (2024-01-16)")
df_v2 = gen.ventas_fecha(df_inv, date(2024, 1, 16), n=60)
TableWriter(ct_ventas).overwrite_partition(df_v2, partition={"fecha": "2024-01-16"})
print(f"    ✔ {df_v2.count()} ventas del 2024-01-16 escritas")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2 — TableReader.read(): lectura completa y filtrada
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 2 — TableReader.read()")

reader_inv = TableReader(ct_inventario)
reader_ven = TableReader(ct_ventas)

_sub("Lectura completa del inventario")
df_full = reader_inv.read()
print(f"    Filas leídas: {df_full.count()}")
print(f"    Columnas    : {df_full.columns}")

_sub("Lectura filtrada — solo ELECTRONICA activa")
df_electro = reader_inv.read(filter="categoria = 'ELECTRONICA' AND activo = true")
print(f"    Productos de electrónica activos: {df_electro.count()}")
df_electro.select("producto_id", "nombre", "stock", "precio_usd").show(truncate=False)

_sub("Lectura con proyección y límite — top 5 precios")
df_top = reader_inv.read(
    columns=["producto_id", "nombre", "precio_usd"],
    limit=5,
)
print(f"    Columnas seleccionadas: {df_top.columns}")
df_top.orderBy("precio_usd", ascending=False).show(truncate=False)

_sub("Resultado es un DataFrame nativo — se puede seguir transformando")
resumen_cat = (
    reader_inv.read()            # TableReader retorna DataFrame real
    .groupBy("categoria")
    .agg(
        {"stock": "sum", "precio_usd": "avg"}
    )
    .orderBy("categoria")
)
print("    Resumen por categoría (groupBy nativo de PySpark):")
resumen_cat.show(truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3 — TableReader.read_partition(): lectura por partición
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 3 — TableReader.read_partition()")

_sub("Ventas del 2024-01-15 (solo esa partición)")
df_part = reader_ven.read_partition({"fecha": "2024-01-15"})
print(f"    Filas en partición 2024-01-15: {df_part.count()}")
df_part.groupBy("categoria").sum("unidades_vendidas").orderBy("categoria").show(truncate=False)

_sub("Intentar leer por columna que NO es partición — debe lanzar ValueError")
try:
    reader_ven.read_partition({"producto_id": "P001"})
except ValueError as e:
    print(f"    ✔ Error capturado correctamente: {e}")

_sub("Lectura de inventario por categoría HOGAR")
df_hogar = reader_inv.read_partition({"categoria": "HOGAR"})
print(f"    Productos en HOGAR: {df_hogar.count()}")
df_hogar.select("producto_id", "nombre", "stock").show(truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4 — Change Data Feed: upsert y captura de cambios
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 4 — Change Data Feed (CDF)")

_sub("Versión actual del inventario antes de la actualización")
version_antes = spark.sql(
    f"DESCRIBE HISTORY {ct_inventario.effective_name}"
).select("version").first()[0]
print(f"    Versión actual: {version_antes}")

_sub("Aplicando actualizaciones de stock (upsert — versión siguiente)")
df_actualizaciones = gen.actualizaciones_stock(df_inv)
print(f"    Productos a actualizar/desactivar: {df_actualizaciones.count()}")
TableWriter(ct_inventario).upsert(df_actualizaciones, keys=["producto_id"])

version_despues = spark.sql(
    f"DESCRIBE HISTORY {ct_inventario.effective_name}"
).select("version").first()[0]
print(f"    Nueva versión después del upsert: {version_despues}")

_sub(f"Leyendo CDF desde versión {version_antes + 1} hasta {version_despues}")
df_cdf = reader_inv.read_cdf(starting_version=version_antes + 1)

print(f"\n    Cambios capturados por CDF: {df_cdf.count()} entradas")
print("    Tipos de cambio (_change_type):")
df_cdf.groupBy("_change_type").count().orderBy("_change_type").show()

print("    Detalle de filas cambiadas:")
df_cdf.select(
    "producto_id", "nombre", "stock", "activo",
    "_change_type", "_commit_version"
).orderBy("producto_id", "_change_type").show(truncate=False)

_sub("CDF con rango de versiones — solo entre versiones específicas")
if version_despues > 0:
    df_cdf_rango = reader_inv.read_cdf(
        starting_version=0,
        ending_version=version_despues,
    )
    print(f"    Entradas totales en el CDF (v0 → v{version_despues}): {df_cdf_rango.count()}")

_sub("Intentar read_cdf sin change_data_feed habilitado — debe lanzar ValueError")
try:
    TableReader(ct_ventas).read_cdf(starting_version=0)
except ValueError as e:
    print(f"    ✔ Error capturado correctamente: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 5 — read_stream(): Structured Streaming
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 5 — TableReader.read_stream()")

_sub("Creando streaming DataFrame desde el inventario")
stream_df = reader_inv.read_stream()
print(f"    stream_df.isStreaming = {stream_df.isStreaming}")
print(f"    Schema del stream     = {[f.name for f in stream_df.schema.fields]}")

_sub("Procesando stream con foreachBatch (micro-batch a memoria)")
batch_count = {"n": 0, "rows": 0}

def procesar_batch(batch_df, batch_id: int) -> None:
    n = batch_df.count()
    batch_count["n"]    += 1
    batch_count["rows"] += n
    print(f"      Batch {batch_id}: {n} filas | categorías: {[r[0] for r in batch_df.select('categoria').distinct().collect()]}")

query = (
    stream_df.writeStream
    .foreachBatch(procesar_batch)
    .trigger(availableNow=True)   # procesa todo lo disponible y termina
    .start()
)
query.awaitTermination(timeout=30)
print(f"\n    ✔ Stream procesado: {batch_count['n']} batches | {batch_count['rows']} filas totales")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 6 — Resumen
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 6 — Resumen del demo")

total_inv   = reader_inv.read().count()
total_ven   = reader_ven.read().count()
total_cdf   = df_cdf.count()

print(f"""
    ┌──────────────────────────────────────────────────────────────┐
    │         RESUMEN — Demo 4: TableReader + CDF                  │
    ├──────────────────────────────────────────────────────────────┤
    │  Productos en inventario        : {total_inv:>6}                   │
    │  Ventas totales (ambos días)    : {total_ven:>6}                   │
    │  Entradas en CDF (upsert)       : {total_cdf:>6}                   │
    │  Versión final del inventario   : {version_despues:>6}                   │
    ├──────────────────────────────────────────────────────────────┤
    │  read()           ✔  DataFrame nativo con filter/columns     │
    │  read_partition() ✔  Lectura eficiente por partición         │
    │  read_stream()    ✔  Structured Streaming con foreachBatch   │
    │  read_cdf()       ✔  Cambios capturados por versión          │
    └──────────────────────────────────────────────────────────────┘
""")

print("✔ Demo 4 completado exitosamente\n")
