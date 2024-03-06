from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from rubicon_ml.domain.utils import TrainingMetadata, uuid


@dataclass
class Project:
    name: str

    id: str = field(default_factory=uuid.uuid4)
    description: Optional[str] = None
    github_url: Optional[str] = None
    training_metadata: Optional[TrainingMetadata] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
