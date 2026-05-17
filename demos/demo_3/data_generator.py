"""
data_generator.py
=================
Generador de datos sintéticos para el demo_3 (e-commerce).
"""

from __future__ import annotations

import random
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType,
)


PAISES   = ["CO", "MX", "PE", "AR", "CL", "EC", "VE"]
ESTADOS  = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
METODOS  = ["STANDARD", "EXPRESS", "PICKUP"]
SEGMENTOS = ["VIP", "REGULAR", "NEW"]


def _rand_date(start: str = "2024-01-01", end: str = "2024-06-30") -> date:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return s + timedelta(days=random.randint(0, (e - s).days))


class DataGenerator:
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def clientes(self, n: int = 100) -> DataFrame:
        rows = []
        for i in range(1, n + 1):
            rows.append((
                f"C{i:05d}",
                f"Cliente {i}",
                f"cliente{i}@ejemplo.com",
                f"+57 300 {i:07d}",
                random.choice(PAISES),
                random.choice(SEGMENTOS),
                _rand_date("2022-01-01", "2024-01-01"),
            ))

        schema = StructType([
            StructField("cliente_id",      StringType(), False),
            StructField("nombre",          StringType(), False),
            StructField("email",           StringType(), False),
            StructField("telefono",        StringType(), True),
            StructField("pais",            StringType(), False),
            StructField("segmento",        StringType(), True),
            StructField("fecha_registro",  DateType(),   False),
        ])
        return self._spark.createDataFrame(rows, schema)

    def pedidos_v1(self, n: int = 300) -> DataFrame:
        """Pedidos con schema inicial (sin columnas de envío ni calificación)."""
        rows = []
        for i in range(1, n + 1):
            cliente_id = f"C{random.randint(1, 100):05d}"
            rows.append((
                f"P{i:06d}",
                cliente_id,
                f"{cliente_id.lower()}@ejemplo.com",
                _rand_date("2024-01-01", "2024-03-31"),
                round(random.uniform(10.0, 500.0), 2),
                random.choice(ESTADOS),
            ))

        schema = StructType([
            StructField("pedido_id",      StringType(), False),
            StructField("cliente_id",     StringType(), False),
            StructField("email_cliente",  StringType(), True),
            StructField("fecha_pedido",   DateType(),   False),
            StructField("total_usd",      DoubleType(), False),
            StructField("estado",         StringType(), False),
        ])
        return self._spark.createDataFrame(rows, schema)

    def pedidos_v2(self, n: int = 200) -> DataFrame:
        """Pedidos con schema evolucionado — incluye metodo_envio, dias_entrega, calificacion."""
        rows = []
        for i in range(301, 301 + n):
            cliente_id = f"C{random.randint(1, 100):05d}"
            estado = random.choice(ESTADOS)
            rows.append((
                f"P{i:06d}",
                cliente_id,
                f"{cliente_id.lower()}@ejemplo.com",
                _rand_date("2024-04-01", "2024-06-30"),
                round(random.uniform(10.0, 500.0), 2),
                estado,
                random.choice(METODOS),
                random.randint(1, 15) if estado == "DELIVERED" else None,
                random.randint(1, 5)  if estado == "DELIVERED" else None,
            ))

        schema = StructType([
            StructField("pedido_id",       StringType(),  False),
            StructField("cliente_id",      StringType(),  False),
            StructField("email_cliente",   StringType(),  True),
            StructField("fecha_pedido",    DateType(),    False),
            StructField("total_usd",       DoubleType(),  False),
            StructField("estado",          StringType(),  False),
            StructField("metodo_envio",    StringType(),  True),
            StructField("dias_entrega",    IntegerType(), True),
            StructField("calificacion",    IntegerType(), True),
        ])
        return self._spark.createDataFrame(rows, schema)
