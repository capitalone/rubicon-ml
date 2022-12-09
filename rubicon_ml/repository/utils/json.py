import dataclasses
import json
from base64 import b64decode, b64encode
from datetime import date, datetime

import numpy as np

from rubicon_ml.domain.utils import TrainingMetadata


class DomainJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        """
        Note - if we need to support nested objects
        within dataclasses, we need to leverage asdict()
        """
        if isinstance(obj, datetime):
            return {"_type": "datetime", "value": obj.strftime("%Y-%m-%d %H:%M:%S.%f")}
        elif isinstance(obj, date):
            return {"_type": "date", "value": obj.isoformat()}
        elif isinstance(obj, set):
            return {"_type": "set", "value": list(obj)}
        elif isinstance(obj, TrainingMetadata):
            return {"_type": "training_metadata", "value": obj.training_metadata}
        elif isinstance(obj, (np.generic, np.ndarray)):
            return {
                "_type": "numpy",
                "_dtype": np.lib.format.dtype_to_descr(obj.dtype),
                "_shape": obj.shape,
                "value": b64encode(obj.tobytes()).decode(),
            }
        elif dataclasses.is_dataclass(obj):
            return obj.__dict__
        else:
            return super().default(obj)  # pragma: no cover


class DomainJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if obj.get("_type") == "datetime":
            return datetime.strptime(obj.get("value"), "%Y-%m-%d %H:%M:%S.%f")
        elif obj.get("_type") == "date":
            return date.fromisoformat(obj.get("value"))
        elif obj.get("_type") == "set":
            return set(obj.get("value"))
        elif obj.get("_type") == "training_metadata":
            return TrainingMetadata([(*o,) for o in obj.get("value")])
        elif obj.get("_type") == "numpy":
            dtype = np.lib.format.descr_to_dtype(obj.get("_dtype"))
            shape = obj.get("_shape")
            value = np.frombuffer(b64decode(obj.get("value")), dtype)

            return value.reshape(shape) if shape else value[0]
        else:
            return obj


def dump(data, open_file, **kwargs):
    return json.dump(data, open_file, cls=DomainJSONEncoder, **kwargs)


def dumps(data, **kwargs):
    return json.dumps(data, cls=DomainJSONEncoder, **kwargs)


def load(open_file, **kwargs):
    return json.load(open_file, cls=DomainJSONDecoder, **kwargs)


def loads(data, **kwargs):
    return json.loads(data, cls=DomainJSONDecoder, **kwargs)
