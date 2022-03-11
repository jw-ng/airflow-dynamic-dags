import json
import os
import typing

SOURCES = "SOURCES"
DEFAULT_SOURCES_ENV_VAR = "[]"


def get_sources() -> typing.List[str]:
    sources_env_var = os.getenv(SOURCES, DEFAULT_SOURCES_ENV_VAR)
    sources = json.loads(sources_env_var)
    return sources
