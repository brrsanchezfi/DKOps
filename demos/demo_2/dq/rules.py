"""
rules.py
========
Reglas de Data Quality declarativas para cada tabla del demo.

Las reglas viven aquí (Python) por simplicidad — en un proyecto real
podrían estar en YAML/JSON al lado del contrato. La idea es que sean
DECLARATIVAS: describen QUÉ debe cumplirse, no CÓMO verificarlo.

Severidades:
  · error   → bloquea el pipeline si falla
  · warning → solo logea, no bloquea
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# Silver — Órdenes de producción
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_SILVER_ORDENES = {
    "table": "silver.manufactura.ordenes_produccion",
    "rules": [
        {"type": "not_null",
         "columns": ["orden_id", "linea_id", "producto_id", "fecha_inicio",
                     "estado", "cantidad_planeada", "cantidad_real"]},

        {"type": "unique", "columns": ["orden_id"]},

        {"type": "in_set", "column": "estado",
         "allowed": ["COMPLETED", "IN_PROGRESS", "CANCELLED"]},

        {"type": "in_set", "column": "linea_id",
         "allowed": ["L1", "L2", "L3", "L4"]},

        {"type": "range", "column": "cantidad_planeada", "min": 0},
        {"type": "range", "column": "cantidad_real",     "min": 0},

        # Cumplimiento puede pasarse de 100% (sobreproducción) pero no debería superar 200% — warning
        {"type": "range", "column": "cumplimiento_pct",
         "min": 0, "max": 200, "severity": "warning"},

        # Si la orden está completada, debe tener fecha_fin
        {"type": "expression",
         "name": "completed_tiene_fecha_fin",
         "expression": "estado != 'COMPLETED' OR fecha_fin IS NOT NULL"},

        # fecha_fin >= fecha_inicio si ambas existen
        {"type": "expression",
         "name": "fecha_fin_posterior_inicio",
         "expression": "fecha_fin IS NULL OR fecha_fin >= fecha_inicio"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Silver — Lotes de producción
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_SILVER_LOTES = {
    "table": "silver.manufactura.lotes_produccion",
    "rules": [
        {"type": "not_null",
         "columns": ["lote_id", "orden_id", "producto_id", "fecha_produccion",
                     "cantidad_producida", "cantidad_defectuosa", "resultado_qc"]},

        {"type": "unique", "columns": ["lote_id"]},

        {"type": "in_set", "column": "resultado_qc",
         "allowed": ["APPROVED", "REJECTED", "RETEST"]},

        {"type": "range", "column": "cantidad_producida",  "min": 0},
        {"type": "range", "column": "cantidad_defectuosa", "min": 0},
        {"type": "range", "column": "merma_pct", "min": 0, "max": 100},

        # Defectuosa nunca puede superar producida
        {"type": "expression",
         "name": "defectuosa_le_producida",
         "expression": "cantidad_defectuosa <= cantidad_producida"},

        # cantidad_neta = producida - defectuosa
        {"type": "expression",
         "name": "cantidad_neta_correcta",
         "expression": "cantidad_neta = cantidad_producida - cantidad_defectuosa"},

        # Lotes aprobados deberían tener pH dentro de rango — warning porque puede
        # haber casos válidos (productos no jabonosos)
        {"type": "expression",
         "name": "ph_rango_si_aprobado",
         "expression": "resultado_qc != 'APPROVED' OR ph_dentro_rango = true",
         "severity": "warning"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Silver — Ventas
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_SILVER_VENTAS = {
    "table": "silver.manufactura.ventas",
    "rules": [
        {"type": "not_null",
         "columns": ["venta_id", "fecha", "distribuidor_id", "producto_id",
                     "cantidad", "precio_unitario", "monto_total"]},

        {"type": "unique", "columns": ["venta_id"]},

        {"type": "range", "column": "precio_unitario", "min": 0.01},

        # monto_total = cantidad * precio_unitario (con tolerancia por redondeo)
        {"type": "expression",
         "name": "monto_total_consistente",
         "expression": "abs(monto_total - (cantidad * precio_unitario)) < 0.01"},

        # Si es_devolucion=true → cantidad debe ser <0 O estado original era RETURNED
        # Como ya no tenemos estado_venta en silver, validamos vía cantidad o flag
        {"type": "expression",
         "name": "devolucion_consistente",
         "expression": "es_devolucion = false OR cantidad < 0 OR monto_total < 0",
         "severity": "warning"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Gold — Eficiencia planta
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_GOLD_EFICIENCIA = {
    "table": "gold.manufactura_kpi.eficiencia_planta",
    "rules": [
        {"type": "not_null",
         "columns": ["fecha", "linea_id", "ordenes_completadas", "ordenes_canceladas",
                     "unidades_planeadas", "unidades_producidas"]},

        {"type": "unique", "columns": ["fecha", "linea_id"]},

        {"type": "in_set", "column": "linea_id",
         "allowed": ["L1", "L2", "L3", "L4"]},

        {"type": "range", "column": "ordenes_completadas",   "min": 0},
        {"type": "range", "column": "ordenes_canceladas",    "min": 0},
        {"type": "range", "column": "unidades_planeadas",    "min": 0},
        {"type": "range", "column": "unidades_producidas",   "min": 0},
        {"type": "range", "column": "tiempo_productivo_min", "min": 0},

        {"type": "range", "column": "cumplimiento_pct",
         "min": 0, "max": 200, "severity": "warning"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Gold — Calidad lotes
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_GOLD_CALIDAD = {
    "table": "gold.manufactura_kpi.calidad_lotes",
    "rules": [
        {"type": "not_null",
         "columns": ["anio_mes", "producto_id", "lotes_totales",
                     "lotes_aprobados", "lotes_rechazados"]},

        {"type": "unique", "columns": ["anio_mes", "producto_id"]},

        {"type": "range", "column": "tasa_aprobacion", "min": 0, "max": 100},
        {"type": "range", "column": "merma_pct_prom",  "min": 0, "max": 100},

        # aprobados + rechazados <= totales (puede haber RETEST en medio)
        {"type": "expression",
         "name": "suma_qc_consistente",
         "expression": "lotes_aprobados + lotes_rechazados <= lotes_totales"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Gold — Ventas producto
# ─────────────────────────────────────────────────────────────────────────────

REGLAS_GOLD_VENTAS = {
    "table": "gold.manufactura_kpi.ventas_producto",
    "rules": [
        {"type": "not_null",
         "columns": ["anio_mes", "producto_id", "monto_neto", "ranking_mes"]},

        {"type": "unique", "columns": ["anio_mes", "producto_id"]},

        {"type": "range", "column": "ranking_mes", "min": 1},
    ],
}
