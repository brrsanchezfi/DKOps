"""
pipeline_ecommerce.py
=====================
Demo 3 — Schema evolution y enmascaramiento de columnas.

Muestra dos características clave de DKOps:

  FEATURE 1 — merge_schema: true
    El contrato de pedidos tiene "merge_schema": true.  En el segundo lote
    el DataFrame trae tres columnas nuevas (metodo_envio, dias_entrega,
    calificacion) que no existían en el schema inicial.  AppendWriter activa
    mergeSchema=true automáticamente — Delta añade las columnas sin error y
    sin necesidad de recrear la tabla.

  FEATURE 2 — mask (enmascaramiento de columnas)
    Las tablas clientes y pedidos declaran "mask" en columnas sensibles
    (email, telefono).  En Databricks / Unity Catalog el framework ejecuta
    ALTER TABLE … ALTER COLUMN … SET MASK tras la escritura.  En PC local
    la operación se omite silenciosamente — el pipeline corre sin cambios.

Dominio: plataforma e-commerce.

Ejecutar:
    python pipeline_ecommerce.py
"""

from __future__ import annotations

from DKOps.launcher import Launcher
from DKOps.table_governance import load_contract, TableWriter

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


def _schema_cols(df) -> list[str]:
    return [f.name for f in df.schema.fields]


# ─────────────────────────────────────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────────────────────────────────────

launcher = Launcher("config/config.json")
spark    = launcher.spark
gen      = DataGenerator(spark)


# ─────────────────────────────────────────────────────────────────────────────
# Cargar contratos
# ─────────────────────────────────────────────────────────────────────────────

_sep("Cargando contratos")

ct_clientes   = load_contract("tables/clientes.json")
ct_pedidos_v1 = load_contract("tables/pedidos_v1.json")
ct_pedidos_v2 = load_contract("tables/pedidos_v2.json")

print(f"  ✔ {ct_clientes.effective_name:50s}  merge_schema={ct_clientes.merge_schema}")
print(f"  ✔ {ct_pedidos_v1.effective_name:50s}  merge_schema={ct_pedidos_v1.merge_schema}")

masked_clientes = ct_clientes.masked_columns
masked_pedidos  = ct_pedidos_v1.masked_columns
print(f"\n  Columnas con mask en clientes : {[c.name for c in masked_clientes]}")
print(f"  Columnas con mask en pedidos  : {[c.name for c in masked_pedidos]}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 1 — Carga inicial de clientes y pedidos (schema v1)
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 1 — Carga inicial")

_sub("Generando 100 clientes")
df_clientes = gen.clientes(n=100)
print(f"    Schema v1 de clientes: {_schema_cols(df_clientes)}")
TableWriter(ct_clientes).overwrite(df_clientes)
print(f"    ✔ Tabla clientes creada con {df_clientes.count()} filas")

_sub("Generando 300 pedidos — schema v1 (sin columnas de envío)")
df_pedidos_v1 = gen.pedidos_v1(n=300)
print(f"    Schema v1 de pedidos: {_schema_cols(df_pedidos_v1)}")
TableWriter(ct_pedidos_v1).overwrite(df_pedidos_v1)
print(f"    ✔ Tabla pedidos creada con {df_pedidos_v1.count()} filas")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 2 — Schema evolution: append con columnas nuevas
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 2 — Schema evolution con merge_schema: true")

_sub("Generando 200 pedidos — schema v2 (+ metodo_envio, dias_entrega, calificacion)")
df_pedidos_v2 = gen.pedidos_v2(n=200)
print(f"    Schema v2 de pedidos: {_schema_cols(df_pedidos_v2)}")

nuevas_cols = set(_schema_cols(df_pedidos_v2)) - set(_schema_cols(df_pedidos_v1))
print(f"\n    Columnas nuevas que no estaban en v1: {sorted(nuevas_cols)}")
print(
    "\n    Sin merge_schema esto lanzaría AnalysisException."
    "\n    Con merge_schema=True, Delta añade las columnas automáticamente."
)

TableWriter(ct_pedidos_v2).append(df_pedidos_v2)
print(f"\n    ✔ Append completado — {df_pedidos_v2.count()} filas añadidas")

total = spark.read.table(ct_pedidos_v2.effective_name)
print(f"    ✔ Total en tabla: {total.count()} filas")
print(f"    ✔ Schema final  : {[f.name for f in total.schema.fields]}")

_sub("Verificando schema evolution")
schema_final = {f.name: str(f.dataType) for f in total.schema.fields}
for col in nuevas_cols:
    status = "✔" if col in schema_final else "✘"
    print(f"    {status} columna '{col}' → {schema_final.get(col, 'AUSENTE')}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 3 — Enmascaramiento de columnas (mask)
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 3 — Enmascaramiento de columnas (mask)")

is_databricks = launcher.env._is_databricks
print(f"\n  Entorno actual: {'Databricks' if is_databricks else 'PC local'}")
print(
    "\n  En Databricks / Unity Catalog, el framework ejecuta:"
    "\n    ALTER TABLE ecommerce.clientes ALTER COLUMN email  SET MASK security.mask_email"
    "\n    ALTER TABLE ecommerce.clientes ALTER COLUMN telefono SET MASK security.mask_phone"
    "\n    ALTER TABLE ecommerce.pedidos  ALTER COLUMN email_cliente SET MASK security.mask_email"
)
print(
    "\n  En PC local la operación se omite silenciosamente —"
    "\n  el pipeline corre igual, sin error."
)

if is_databricks:
    print("\n  → Ejecutando ALTER TABLE SET MASK (modo Databricks)...")
else:
    print("\n  → Omitiendo ALTER TABLE SET MASK (modo local, esperado).")

for ct in [ct_clientes, ct_pedidos_v2]:
    masked = ct.masked_columns
    if masked:
        print(f"\n  Contrato '{ct.effective_name}':")
        for col in masked:
            print(f"    • {col.name:20s} → mask = {col.mask}")


# ─────────────────────────────────────────────────────────────────────────────
# FASE 4 — Consultas de negocio
# ─────────────────────────────────────────────────────────────────────────────

_sep("FASE 4 — Consultas de negocio")

_sub("Top 5 clientes por gasto total (todos los pedidos)")
spark.sql(f"""
    SELECT p.cliente_id,
           COUNT(*)                  AS total_pedidos,
           ROUND(SUM(p.total_usd),2) AS gasto_total_usd,
           MIN(p.fecha_pedido)       AS primer_pedido,
           MAX(p.fecha_pedido)       AS ultimo_pedido
    FROM {ct_pedidos_v2.effective_name} p
    WHERE p.estado NOT IN ('CANCELLED')
    GROUP BY p.cliente_id
    ORDER BY gasto_total_usd DESC
    LIMIT 5
""").show(truncate=False)

_sub("Distribución de pedidos por método de envío (solo v2)")
spark.sql(f"""
    SELECT metodo_envio,
           COUNT(*)                    AS pedidos,
           ROUND(AVG(total_usd), 2)    AS ticket_promedio_usd,
           ROUND(AVG(dias_entrega), 1) AS dias_entrega_prom,
           ROUND(AVG(calificacion), 2) AS calificacion_prom
    FROM {ct_pedidos_v2.effective_name}
    WHERE metodo_envio IS NOT NULL
    GROUP BY metodo_envio
    ORDER BY pedidos DESC
""").show(truncate=False)

_sub("Distribución de estados (schema completo)")
spark.sql(f"""
    SELECT estado,
           COUNT(*)                 AS pedidos,
           ROUND(SUM(total_usd),2)  AS monto_total_usd
    FROM {ct_pedidos_v2.effective_name}
    GROUP BY estado
    ORDER BY pedidos DESC
""").show(truncate=False)

_sub("Resumen del demo")
resumen = spark.sql(f"""
    SELECT
      (SELECT COUNT(*) FROM {ct_clientes.effective_name})   AS total_clientes,
      (SELECT COUNT(*) FROM {ct_pedidos_v2.effective_name}) AS total_pedidos,
      (SELECT COUNT(*) FROM {ct_pedidos_v2.effective_name}
       WHERE metodo_envio IS NOT NULL)                      AS pedidos_v2,
      (SELECT COUNT(*) FROM {ct_pedidos_v2.effective_name}
       WHERE metodo_envio IS NULL)                          AS pedidos_v1
""").collect()[0]

print(f"""
    ┌─────────────────────────────────────────────────────────────┐
    │          RESUMEN — Demo 3: schema evolution + masks         │
    ├─────────────────────────────────────────────────────────────┤
    │  Clientes cargados          : {resumen.total_clientes:>6}                    │
    │  Pedidos v1 (schema inicial): {resumen.pedidos_v1:>6}                    │
    │  Pedidos v2 (schema nuevo)  : {resumen.pedidos_v2:>6}                    │
    │  Total pedidos en tabla     : {resumen.total_pedidos:>6}                    │
    ├─────────────────────────────────────────────────────────────┤
    │  merge_schema activó la evolución de schema automáticamente │
    │  mask declarado en email y telefono de clientes             │
    │  mask declarado en email_cliente de pedidos                 │
    └─────────────────────────────────────────────────────────────┘
""")

print("✔ Demo 3 completado exitosamente\n")
