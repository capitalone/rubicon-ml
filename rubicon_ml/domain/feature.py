from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from rubicon_ml.domain.mixin import TagMixin
from rubicon_ml.domain.utils import uuid


@dataclass
class Feature(TagMixin):
    name: str

    id: str = field(default_factory=uuid.uuid4)
    description: str = None
    importance: float = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
