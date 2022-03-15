import typing
from importlib import import_module


def get_sources() -> typing.List[str]:
    try:
        sources_module = import_module("configs.sources")
    except ModuleNotFoundError:
        return []
    else:
        return sources_module.__getattribute__("SOURCES")
