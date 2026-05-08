"""
Funciones puras de transformación. Cada función recibe DataFrame(s)
y devuelve un DataFrame — sin side effects ni dependencias globales.
"""

from transformations.bronze_to_silver import (
    transformar_ordenes_silver,
    transformar_lotes_silver,
    transformar_ventas_silver,
    normalizar_estado_orden,
    normalizar_resultado_qc,
    deduplicar_ordenes,
)
from transformations.silver_to_gold import (
    kpi_eficiencia_planta,
    kpi_calidad_lotes,
    kpi_ventas_producto,
)

__all__ = [
    "transformar_ordenes_silver",
    "transformar_lotes_silver",
    "transformar_ventas_silver",
    "normalizar_estado_orden",
    "normalizar_resultado_qc",
    "deduplicar_ordenes",
    "kpi_eficiencia_planta",
    "kpi_calidad_lotes",
    "kpi_ventas_producto",
]