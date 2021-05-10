from dataclasses import dataclass, field
from datetime import datetime

from rubicon_ml.domain.utils import uuid


@dataclass
class Parameter:
    name: str

    id: str = field(default_factory=uuid.uuid4)
    value: object = None
    description: str = None
    created_at: datetime = field(default_factory=datetime.utcnow)
