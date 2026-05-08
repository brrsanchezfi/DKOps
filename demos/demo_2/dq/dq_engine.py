"""
dq_engine.py
============
Motor de Data Quality declarativo — local al demo.

Diseño minimalista pero robusto: las reglas se declaran como dicts
(o YAML cargado a dict) y se ejecutan contra un DataFrame. Cada regla
devuelve un `RuleResult` con: pasó / no pasó, filas que fallan, severidad.

Si el demo demuestra valor, este módulo se puede graduar a `DKOps.data_quality`
sin cambios estructurales — los `Rule` y `RuleSet` están hechos para vivir
fuera del demo.

Ejemplo de uso
--------------
    from demo_2.dq.dq_engine import RuleSet

    rules = RuleSet.from_dict({
        "table": "silver.manufactura.ordenes_produccion",
        "rules": [
            {"type": "not_null",  "columns": ["orden_id", "linea_id"]},
            {"type": "unique",    "columns": ["orden_id"]},
            {"type": "in_set",    "column": "estado",
             "allowed": ["COMPLETED", "IN_PROGRESS", "CANCELLED"]},
            {"type": "range",     "column": "cumplimiento_pct",
             "min": 0, "max": 200, "severity": "warning"},
        ],
    })
    report = rules.run(df_silver)
    report.raise_if_failed()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Severidades
# ─────────────────────────────────────────────────────────────────────────────

SEVERITY_ERROR   = "error"     # falla bloquea el pipeline
SEVERITY_WARNING = "warning"   # falla solo logea
SEVERITIES = {SEVERITY_ERROR, SEVERITY_WARNING}


# ─────────────────────────────────────────────────────────────────────────────
# Resultado de una regla
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RuleResult:
    rule_name:    str
    passed:       bool
    severity:     str
    failed_count: int    = 0
    total_count:  int    = 0
    message:      str    = ""

    def __str__(self) -> str:
        icon = "✔" if self.passed else ("✖" if self.severity == SEVERITY_ERROR else "⚠")
        pct  = (self.failed_count / self.total_count * 100) if self.total_count else 0
        return (
            f"  {icon} [{self.severity:7s}] {self.rule_name:50s} "
            f"failed={self.failed_count:>5d}/{self.total_count} ({pct:5.1f}%)"
            + (f"  — {self.message}" if self.message else "")
        )


# ─────────────────────────────────────────────────────────────────────────────
# Reglas individuales
# ─────────────────────────────────────────────────────────────────────────────

class Rule:
    """Clase base. Cada subclase implementa `evaluate(df) -> RuleResult`."""

    def __init__(self, severity: str = SEVERITY_ERROR) -> None:
        if severity not in SEVERITIES:
            raise ValueError(f"severity debe ser uno de {SEVERITIES}, recibido: {severity}")
        self.severity = severity

    def evaluate(self, df: DataFrame) -> RuleResult:
        raise NotImplementedError


class NotNullRule(Rule):
    """Una o más columnas no pueden tener NULL."""

    def __init__(self, columns: list[str], severity: str = SEVERITY_ERROR) -> None:
        super().__init__(severity)
        self.columns = columns

    def evaluate(self, df: DataFrame) -> RuleResult:
        total = df.count()
        cond = None
        for c in self.columns:
            null_cond = F.col(c).isNull()
            cond = null_cond if cond is None else (cond | null_cond)
        failed = df.where(cond).count() if cond is not None else 0
        return RuleResult(
            rule_name    = f"not_null({', '.join(self.columns)})",
            passed       = failed == 0,
            severity     = self.severity,
            failed_count = failed,
            total_count  = total,
        )


class UniqueRule(Rule):
    """La combinación de columnas debe ser única (PK lógica)."""

    def __init__(self, columns: list[str], severity: str = SEVERITY_ERROR) -> None:
        super().__init__(severity)
        self.columns = columns

    def evaluate(self, df: DataFrame) -> RuleResult:
        total = df.count()
        dups = (
            df.groupBy(*self.columns)
              .count()
              .where(F.col("count") > 1)
              .count()
        )
        return RuleResult(
            rule_name    = f"unique({', '.join(self.columns)})",
            passed       = dups == 0,
            severity     = self.severity,
            failed_count = dups,
            total_count  = total,
            message      = f"{dups} grupo(s) con duplicados" if dups else "",
        )


class InSetRule(Rule):
    """Los valores de una columna deben estar en un conjunto permitido."""

    def __init__(
        self,
        column:   str,
        allowed:  list[Any],
        severity: str = SEVERITY_ERROR,
    ) -> None:
        super().__init__(severity)
        self.column  = column
        self.allowed = allowed

    def evaluate(self, df: DataFrame) -> RuleResult:
        total  = df.count()
        failed = df.where(~F.col(self.column).isin(self.allowed)).count()
        return RuleResult(
            rule_name    = f"in_set({self.column} ∈ {self.allowed})",
            passed       = failed == 0,
            severity     = self.severity,
            failed_count = failed,
            total_count  = total,
        )


class RangeRule(Rule):
    """Una columna numérica debe estar dentro de un rango [min, max]."""

    def __init__(
        self,
        column:    str,
        min:       float | None = None,
        max:       float | None = None,
        severity:  str = SEVERITY_ERROR,
    ) -> None:
        super().__init__(severity)
        if min is None and max is None:
            raise ValueError("RangeRule requiere al menos `min` o `max`.")
        self.column = column
        self.min    = min
        self.max    = max

    def evaluate(self, df: DataFrame) -> RuleResult:
        total = df.count()
        cond = F.lit(False)
        if self.min is not None:
            cond = cond | (F.col(self.column) < self.min)
        if self.max is not None:
            cond = cond | (F.col(self.column) > self.max)
        # No contamos NULLs como fuera de rango — eso es responsabilidad de NotNullRule
        cond = cond & F.col(self.column).isNotNull()
        failed = df.where(cond).count()
        return RuleResult(
            rule_name    = f"range({self.column} ∈ [{self.min}, {self.max}])",
            passed       = failed == 0,
            severity     = self.severity,
            failed_count = failed,
            total_count  = total,
        )


class ExpressionRule(Rule):
    """
    Regla custom basada en una expresión SQL booleana.
    La regla pasa si TODAS las filas evalúan a True.
    """

    def __init__(
        self,
        name:        str,
        expression:  str,
        severity:    str = SEVERITY_ERROR,
    ) -> None:
        super().__init__(severity)
        self.name       = name
        self.expression = expression

    def evaluate(self, df: DataFrame) -> RuleResult:
        total  = df.count()
        # Filas que NO cumplen la expresión (incluyendo cuando es NULL)
        failed = df.where(~F.expr(self.expression) | F.expr(self.expression).isNull()).count()
        return RuleResult(
            rule_name    = f"expression[{self.name}]",
            passed       = failed == 0,
            severity     = self.severity,
            failed_count = failed,
            total_count  = total,
            message      = self.expression,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Factory para construir reglas desde dict
# ─────────────────────────────────────────────────────────────────────────────

_RULE_TYPES = {
    "not_null":   NotNullRule,
    "unique":     UniqueRule,
    "in_set":     InSetRule,
    "range":      RangeRule,
    "expression": ExpressionRule,
}


def build_rule(spec: dict) -> Rule:
    """
    Construye una regla desde un dict de especificación.

    Ej:
        {"type": "not_null", "columns": ["orden_id"], "severity": "error"}
    """
    spec = dict(spec)  # copia para no mutar
    rule_type = spec.pop("type")
    if rule_type not in _RULE_TYPES:
        raise ValueError(
            f"Tipo de regla '{rule_type}' no reconocido. "
            f"Disponibles: {sorted(_RULE_TYPES.keys())}"
        )
    return _RULE_TYPES[rule_type](**spec)


# ─────────────────────────────────────────────────────────────────────────────
# Reporte agregado y RuleSet
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DQReport:
    table:   str
    results: list[RuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def has_errors(self) -> bool:
        """¿Alguna regla con severity=error falló?"""
        return any((not r.passed) and r.severity == SEVERITY_ERROR for r in self.results)

    @property
    def warnings(self) -> list[RuleResult]:
        return [r for r in self.results if (not r.passed) and r.severity == SEVERITY_WARNING]

    @property
    def errors(self) -> list[RuleResult]:
        return [r for r in self.results if (not r.passed) and r.severity == SEVERITY_ERROR]

    def print(self) -> None:
        print(f"\n  DQ Report — {self.table} ({len(self.results)} regla(s)):")
        print("  " + "─" * 80)
        for r in self.results:
            print(r)
        print("  " + "─" * 80)
        status = "PASSED" if self.passed else ("FAILED" if self.has_errors else "PASSED (con warnings)")
        print(f"  Status: {status} | errors={len(self.errors)} | warnings={len(self.warnings)}\n")

    def raise_if_failed(self) -> None:
        """Lanza ValueError si hay errores. Los warnings no bloquean."""
        if self.has_errors:
            lines = "\n  ".join(str(r) for r in self.errors)
            raise ValueError(
                f"DQ falló para tabla '{self.table}':\n  {lines}"
            )


@dataclass
class RuleSet:
    table: str
    rules: list[Rule] = field(default_factory=list)

    @classmethod
    def from_dict(cls, spec: dict) -> "RuleSet":
        """
        Construye un RuleSet desde un dict (típicamente cargado de YAML).

        Formato esperado:
            {
              "table": "silver.manufactura.ordenes_produccion",
              "rules": [
                {"type": "not_null", "columns": [...]},
                ...
              ]
            }
        """
        return cls(
            table = spec["table"],
            rules = [build_rule(r) for r in spec.get("rules", [])],
        )

    def run(self, df: DataFrame) -> DQReport:
        results = [rule.evaluate(df) for rule in self.rules]
        return DQReport(table=self.table, results=results)
