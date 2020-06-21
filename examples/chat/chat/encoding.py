import cattr
from datetime import datetime
import importlib
from typing import Any

cattr.register_structure_hook(datetime, lambda d, t: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%f%z"))
cattr.register_unstructure_hook(datetime, lambda d: d.isoformat())


def encode(event: Any) -> (str, dict):
    klass = event.__class__
    topic_id = f"{klass.__module__}#{getattr(klass, '__qualname__', klass.__name__)}"
    return topic_id, cattr.unstructure(event)


def decode(topic_id: str, data: dict) -> Any:
    module_name, _, class_name = topic_id.partition("#")
    module = importlib.import_module(module_name)
    # TODO: support nested classes
    klass = getattr(module, class_name)
    return cattr.structure(data, klass)
