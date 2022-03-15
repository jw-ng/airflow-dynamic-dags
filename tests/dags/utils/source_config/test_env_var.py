import os

import pytest

from utils.source_config.env_var import SOURCES, get_sources


def test_get_sources_return_empty_list_when_env_var_does_not_exist():
    assert os.getenv(SOURCES) is None

    assert get_sources() == []


@pytest.mark.parametrize(
    "env_var_value, expected_list",
    [
        ("[]", []),
        ('["foo"]', ["foo"]),
        ('["foo", "bar"]', ["foo", "bar"]),
    ],
)
def test_get_sources_return_deserialised_value_of_env_var(
    mocker, env_var_value, expected_list
):
    mocker.patch.dict("os.environ", SOURCES=env_var_value)

    assert get_sources() == expected_list
