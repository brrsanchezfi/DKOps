"""
Módulo de Data Quality declarativo (local al demo-2).
"""

from dq.dq_engine import (
    Rule,
    RuleSet,
    DQReport,
    RuleResult,
    NotNullRule,
    UniqueRule,
    InSetRule,
    RangeRule,
    ExpressionRule,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
)

__all__ = [
    "Rule", "RuleSet", "DQReport", "RuleResult",
    "NotNullRule", "UniqueRule", "InSetRule", "RangeRule", "ExpressionRule",
    "SEVERITY_ERROR", "SEVERITY_WARNING",
]