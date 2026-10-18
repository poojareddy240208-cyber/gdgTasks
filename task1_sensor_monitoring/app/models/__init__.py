"""ORM model registry.

Import everything from here so that Alembic ``autogenerate`` and test
``create_all`` calls discover every mapped table via ``Base.metadata``.
"""

from app.models.base import Base
from app.models.alert_rule import AlertRule, RuleType
from app.models.alert import Alert, AlertStatus

__all__ = [
    "Base",
    "AlertRule",
    "RuleType",
    "Alert",
    "AlertStatus",
]
