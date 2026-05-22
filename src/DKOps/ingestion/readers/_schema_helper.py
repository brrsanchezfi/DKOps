"""_schema_helper.py — Convierte lista de dicts {name, type} a StructType de Spark."""

from __future__ import annotations

from pyspark.sql.types import (
    BinaryType, BooleanType, DateType, DoubleType,
    FloatType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

_TYPE_MAP: dict[str, object] = {
    "string":    StringType(),
    "str":       StringType(),
    "integer":   IntegerType(),
    "int":       IntegerType(),
    "long":      LongType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "boolean":   BooleanType(),
    "bool":      BooleanType(),
    "timestamp": TimestampType(),
    "date":      DateType(),
    "binary":    BinaryType(),
}


def build_spark_schema(schema_list: list[dict]) -> StructType:
    """Convierte [{name, type}, ...] a StructType."""
    fields = []
    for col in schema_list:
        name  = col["name"]
        ctype = col["type"].lower()
        spark_type = _TYPE_MAP.get(ctype)
        if spark_type is None:
            raise ValueError(
                f"Tipo '{ctype}' de columna '{name}' no reconocido en schema hint.\n"
                f"Tipos soportados: {sorted(_TYPE_MAP.keys())}"
            )
        fields.append(StructField(name, spark_type, nullable=True))
    return StructType(fields)
