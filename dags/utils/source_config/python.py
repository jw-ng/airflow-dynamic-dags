import typing
from importlib import import_module

from constants import SOURCES_ATTRIBUTE_NAME, SOURCES_MODULE


def get_sources() -> typing.List[str]:
    try:
        sources_module = import_module(SOURCES_MODULE)
    except ModuleNotFoundError:
        return []
    else:
        return sources_module.__getattribute__(SOURCES_ATTRIBUTE_NAME)
