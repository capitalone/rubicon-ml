from dataclasses import dataclass, field
from datetime import datetime

from rubicon_ml.domain.utils import uuid


@dataclass
class Feature:
    name: str

    id: str = field(default_factory=uuid.uuid4)
    description: str = None
    importance: float = None
    created_at: datetime = field(default_factory=datetime.utcnow)
