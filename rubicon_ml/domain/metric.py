from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from rubicon_ml.domain.mixin import TagMixin
from rubicon_ml.domain.utils import uuid

DIRECTIONALITY_VALUES = ["score", "loss"]


@dataclass
class Metric(TagMixin):
    name: str
    value: float

    id: str = field(default_factory=uuid.uuid4)
    description: str = None
    directionality: str = "score"
    created_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.directionality not in DIRECTIONALITY_VALUES:
            raise ValueError(f"metric directionality must be one of {DIRECTIONALITY_VALUES}")
