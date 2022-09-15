from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from rubicon_ml.domain.mixin import TagMixin
from rubicon_ml.domain.utils import uuid


@dataclass
class Artifact(TagMixin):
    name: str

    id: str = field(default_factory=uuid.uuid4)
    description: str = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)

    parent_id: str = None
