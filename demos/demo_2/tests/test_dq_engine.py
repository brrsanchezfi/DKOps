"""
test_dq_engine.py
=================
Tests del motor de Data Quality.

Validamos que:
  · Cada tipo de regla (NotNull, Unique, InSet, Range, Expression) detecta correctamente.
  · La construcción desde dict funciona (Factory).
  · El RuleSet ejecuta múltiples reglas y reporta resultados agregados.
  · `raise_if_failed` solo bloquea con errores (no con warnings).
"""

from __future__ import annotations

import pytest
from pyspark.sql import types as T

from demo_2.dq.dq_engine import (
    NotNullRule, UniqueRule, InSetRule, RangeRule, ExpressionRule,
    RuleSet, build_rule,
    SEVERITY_ERROR, SEVERITY_WARNING,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures locales — DataFrames pequeños para los tests de reglas
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def df_personas(spark):
    schema = T.StructType([
        T.StructField("id",     T.IntegerType(), True),
        T.StructField("nombre", T.StringType(),  True),
        T.StructField("edad",   T.IntegerType(), True),
        T.StructField("tipo",   T.StringType(),  True),
    ])
    rows = [
        (1, "Ana",   30, "EMPLEADO"),
        (2, "Pedro", 45, "EMPLEADO"),
        (3, "Luis",  None, "CLIENTE"),
        (4, None,    25, "CLIENTE"),
        (5, "María", 999, "OTRO"),       # edad outlier, tipo no permitido
    ]
    return spark.createDataFrame(rows, schema)


# ═════════════════════════════════════════════════════════════════════════════
# NotNullRule
# ═════════════════════════════════════════════════════════════════════════════

class TestNotNullRule:

    def test_pasa_cuando_no_hay_nulos(self, spark):
        rows = [(1, "Ana"), (2, "Pedro")]
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("nombre", T.StringType()),
        ])
        df = spark.createDataFrame(rows, schema)

        result = NotNullRule(["id", "nombre"]).evaluate(df)
        assert result.passed
        assert result.failed_count == 0

    def test_falla_si_hay_nulos(self, df_personas):
        result = NotNullRule(["nombre"]).evaluate(df_personas)
        assert not result.passed
        assert result.failed_count == 1   # solo id=4 tiene nombre null

    def test_combina_multiples_columnas(self, df_personas):
        # Una fila falla si CUALQUIERA de las columnas tiene null
        result = NotNullRule(["nombre", "edad"]).evaluate(df_personas)
        # id=3 tiene edad null, id=4 tiene nombre null → 2 filas
        assert result.failed_count == 2


# ═════════════════════════════════════════════════════════════════════════════
# UniqueRule
# ═════════════════════════════════════════════════════════════════════════════

class TestUniqueRule:

    def test_pasa_si_unicos(self, df_personas):
        result = UniqueRule(["id"]).evaluate(df_personas)
        assert result.passed

    def test_detecta_duplicados(self, spark):
        rows = [(1, "A"), (1, "A"), (2, "B")]
        schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("v",  T.StringType()),
        ])
        df = spark.createDataFrame(rows, schema)

        result = UniqueRule(["id"]).evaluate(df)
        assert not result.passed
        assert result.failed_count == 1   # un grupo con duplicados


# ═════════════════════════════════════════════════════════════════════════════
# InSetRule
# ═════════════════════════════════════════════════════════════════════════════

class TestInSetRule:

    def test_detecta_valor_no_permitido(self, df_personas):
        result = InSetRule(
            column  = "tipo",
            allowed = ["EMPLEADO", "CLIENTE"],
        ).evaluate(df_personas)

        assert not result.passed
        assert result.failed_count == 1   # solo "OTRO" no está permitido

    def test_pasa_si_todo_dentro(self, spark):
        rows = [("A",), ("B",), ("A",)]
        df = spark.createDataFrame(rows, T.StructType([T.StructField("v", T.StringType())]))

        result = InSetRule(column="v", allowed=["A", "B"]).evaluate(df)
        assert result.passed


# ═════════════════════════════════════════════════════════════════════════════
# RangeRule
# ═════════════════════════════════════════════════════════════════════════════

class TestRangeRule:

    def test_solo_min(self, df_personas):
        # edades >= 0 → todas válidas (excepto las null que ignoramos)
        result = RangeRule(column="edad", min=0).evaluate(df_personas)
        assert result.passed

    def test_min_y_max(self, df_personas):
        # edades 0-120 → la edad 999 falla
        result = RangeRule(column="edad", min=0, max=120).evaluate(df_personas)
        assert not result.passed
        assert result.failed_count == 1

    def test_ignora_nulls(self, df_personas):
        # id=3 tiene edad NULL — no debe contar como out-of-range.
        # Subimos max a 2000 para que 999 esté dentro; si la implementación contara
        # los NULL, este test fallaría con failed_count=1.
        result = RangeRule(column="edad", min=0, max=2000).evaluate(df_personas)
        assert result.passed
        assert result.failed_count == 0

    def test_requiere_al_menos_un_limite(self):
        with pytest.raises(ValueError):
            RangeRule(column="x")


# ═════════════════════════════════════════════════════════════════════════════
# ExpressionRule
# ═════════════════════════════════════════════════════════════════════════════

class TestExpressionRule:

    def test_expresion_simple(self, df_personas):
        # edad debe ser razonable (<150)
        result = ExpressionRule(
            name       = "edad_humana",
            expression = "edad < 150",
        ).evaluate(df_personas)

        # 999 falla; null también porque NULL evalúa a NULL → cuenta como falla
        assert not result.passed
        assert result.failed_count == 2  # id=3 (null) e id=5 (999)

    def test_expresion_con_join_logico(self, df_personas):
        # Si tipo=EMPLEADO, edad debe ser >= 18
        result = ExpressionRule(
            name       = "empleado_mayor_edad",
            expression = "tipo != 'EMPLEADO' OR edad >= 18",
        ).evaluate(df_personas)

        # Todos los empleados son adultos → debería pasar
        assert result.passed


# ═════════════════════════════════════════════════════════════════════════════
# build_rule (Factory)
# ═════════════════════════════════════════════════════════════════════════════

class TestBuildRule:

    def test_construye_not_null(self):
        rule = build_rule({"type": "not_null", "columns": ["a", "b"]})
        assert isinstance(rule, NotNullRule)
        assert rule.columns == ["a", "b"]

    def test_construye_con_severity(self):
        rule = build_rule({"type": "range", "column": "x", "min": 0, "severity": "warning"})
        assert isinstance(rule, RangeRule)
        assert rule.severity == SEVERITY_WARNING

    def test_tipo_invalido(self):
        with pytest.raises(ValueError, match="no reconocido"):
            build_rule({"type": "foo_bar"})


# ═════════════════════════════════════════════════════════════════════════════
# RuleSet — integración
# ═════════════════════════════════════════════════════════════════════════════

class TestRuleSet:

    def test_run_ejecuta_todas(self, df_personas):
        rs = RuleSet.from_dict({
            "table": "test.personas",
            "rules": [
                {"type": "not_null", "columns": ["id"]},
                {"type": "unique",   "columns": ["id"]},
                {"type": "in_set",   "column": "tipo", "allowed": ["EMPLEADO", "CLIENTE", "OTRO"]},
            ],
        })
        report = rs.run(df_personas)

        assert len(report.results) == 3
        assert report.passed   # todas estas pasan

    def test_raise_si_hay_error(self, df_personas):
        rs = RuleSet.from_dict({
            "table": "test.personas",
            "rules": [
                {"type": "not_null", "columns": ["nombre"]},
            ],
        })
        report = rs.run(df_personas)

        with pytest.raises(ValueError, match="DQ falló"):
            report.raise_if_failed()

    def test_warning_no_bloquea(self, df_personas):
        rs = RuleSet.from_dict({
            "table": "test.personas",
            "rules": [
                # Esta regla SÍ falla, pero severity=warning
                {"type": "range", "column": "edad", "max": 100, "severity": "warning"},
            ],
        })
        report = rs.run(df_personas)

        # No pasa, pero raise_if_failed no debe lanzar porque no hay errores
        assert not report.passed
        report.raise_if_failed()   # no excepción
        assert len(report.warnings) == 1
        assert len(report.errors)   == 0
