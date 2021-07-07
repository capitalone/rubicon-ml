import dataclasses
import json
from datetime import date, datetime

from rubicon_ml.domain.utils import TrainingMetadata


class DomainJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        Note - if we need to support nested objects
        within dataclasses, we need to leverage asdict()
        """
        if isinstance(obj, datetime):
            return {"_type": "datetime", "value": obj.strftime("%Y-%m-%d %H:%M:%S.%f")}
        if isinstance(obj, date):
            return {"_type": "date", "value": obj.isoformat()}
        if isinstance(obj, set):
            return {"_type": "set", "value": list(obj)}
        if isinstance(obj, TrainingMetadata):
            return {"_type": "training_metadata", "value": obj.training_metadata}
        if dataclasses.is_dataclass(obj):
            return obj.__dict__
        else:
            return super().default(obj)  # pragma: no cover


class DomainJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == "datetime":
            return datetime.strptime(obj.get("value"), "%Y-%m-%d %H:%M:%S.%f")
        if obj.get("_type") == "date":
            return date.fromisoformat(obj.get("value"))
        if obj.get("_type") == "set":
            return set(obj.get("value"))
        if obj.get("_type") == "training_metadata":
            return TrainingMetadata([(*o,) for o in obj.get("value")])
        else:
            return obj


def dumps(data):
    return json.dumps(data, cls=DomainJSONEncoder)


def load(open_file):
    return json.load(open_file, cls=DomainJSONDecoder)


def loads(data):
    return json.loads(data, cls=DomainJSONDecoder)
