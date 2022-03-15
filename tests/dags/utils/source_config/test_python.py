from unittest.mock import MagicMock

import pytest

from utils.source_config.python import get_sources


def test_get_sources_should_return_empty_list_when_module_is_not_found(mocker):
    mocker.patch(
        "utils.source_config.python.import_module",
        side_effect=ModuleNotFoundError("module not found"),
    )

    assert get_sources() == []


@pytest.mark.parametrize(
    "sources",
    [
        [],
        ["foo"],
        ["foo", "bar"],
    ]
)
def test_get_sources_should_return_value_of_sources_variable_in_module(mocker, sources):
    mock_module = MagicMock()
    mock_module.return_value.SOURCES = sources
    mocker.patch("utils.source_config.python.import_module", mock_module)

    assert get_sources() == sources

