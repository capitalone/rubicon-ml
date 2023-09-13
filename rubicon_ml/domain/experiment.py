from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from rubicon_ml.domain.mixin import TagMixin
from rubicon_ml.domain.utils import TrainingMetadata, uuid


@dataclass
class Experiment(TagMixin):
    project_name: str

    id: str = field(default_factory=uuid.uuid4)
    name: Optional[str] = None
    description: Optional[str] = None
    model_name: Optional[str] = None
    branch_name: Optional[str] = None
    commit_hash: Optional[str] = None
    training_metadata: Optional[TrainingMetadata] = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
