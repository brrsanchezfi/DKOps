"""
data_generator.py
=================
Generador de datos sintéticos para el demo_4 (tienda — lectura y CDF).
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType,
)

CATEGORIAS = ["ELECTRONICA", "ROPA", "HOGAR", "ALIMENTOS"]
PRODUCTOS  = {
    "ELECTRONICA": ["Laptop Pro", "Smartphone X", "Auriculares BT", "Tablet Mini", "Smartwatch"],
    "ROPA":        ["Camiseta Algodón", "Jeans Slim", "Chaqueta Invierno", "Vestido Floral"],
    "HOGAR":       ["Cafetera Express", "Licuadora Pro", "Juego Sábanas", "Lámpara LED"],
    "ALIMENTOS":   ["Café Premium", "Aceite Oliva", "Chocolate 70%", "Granola Artesanal"],
}


class DataGenerator:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def inventario_inicial(self) -> DataFrame:
        """Inventario inicial con todos los productos activos."""
        rows = []
        pid  = 1
        for cat, productos in PRODUCTOS.items():
            for nombre in productos:
                rows.append((
                    f"P{pid:03d}",
                    nombre,
                    cat,
                    random.randint(50, 500),
                    round(random.uniform(5.0, 800.0), 2),
                    True,
                ))
                pid += 1

        schema = StructType([
            StructField("producto_id", StringType(),  False),
            StructField("nombre",      StringType(),  False),
            StructField("categoria",   StringType(),  False),
            StructField("stock",       IntegerType(), False),
            StructField("precio_usd",  DoubleType(),  False),
            StructField("activo",      BooleanType(), False),
        ])
        return self._spark.createDataFrame(rows, schema)

    def actualizaciones_stock(self, inventario_df: DataFrame) -> DataFrame:
        """
        Simula actualizaciones de stock: algunos productos reciben reposición
        y algunos se desactivan por agotamiento.
        """
        from pyspark.sql import functions as F

        # Tomar una muestra aleatoria y modificar el stock
        return (
            inventario_df
            .filter("producto_id IN ('P001','P003','P007','P010','P015')")
            .withColumn("stock", F.col("stock") + F.lit(200))
            .withColumn("activo", F.lit(True))
            .union(
                # Productos que se agotan y se desactivan
                inventario_df
                .filter("producto_id IN ('P005','P012')")
                .withColumn("stock", F.lit(0))
                .withColumn("activo", F.lit(False))
            )
        )

    def ventas_fecha(self, inventario_df: DataFrame, fecha: date, n: int = 50) -> DataFrame:
        """Genera n ventas aleatorias para una fecha dada."""
        productos = [r.producto_id for r in inventario_df.select("producto_id", "categoria", "precio_usd").collect()]
        rows = []
        for _ in range(n):
            row = random.choice(inventario_df.select("producto_id", "categoria", "precio_usd").collect())
            unidades = random.randint(1, 10)
            rows.append((
                fecha,
                row.producto_id,
                row.categoria,
                unidades,
                round(row.precio_usd * unidades, 2),
            ))

        schema = StructType([
            StructField("fecha",             DateType(),    False),
            StructField("producto_id",       StringType(),  False),
            StructField("categoria",         StringType(),  False),
            StructField("unidades_vendidas", IntegerType(), False),
            StructField("ingresos_usd",      DoubleType(),  False),
        ])
        return self._spark.createDataFrame(rows, schema)
