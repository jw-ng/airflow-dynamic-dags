from unittest.mock import MagicMock

import pytest

from utils.source_config.mongo import SOURCES_KEY, get_sources


def test_get_sources_return_empty_list_when_no_sources_config_exist_in_mongo_collection(
    mocker,
):
    mock_hook = MagicMock()
    mock_collection = MagicMock()
    mock_collection.find_one.return_value = None
    mock_hook.get_collection.return_value = mock_collection
    mocker.patch("utils.source_config.mongo.MongoHook", return_value=mock_hook)

    assert get_sources() == []


@pytest.mark.parametrize(
    "expected_list",
    [
        [],
        ["foo"],
        ["foo", "bar"],
    ],
)
def test_get_sources_return_sources_config_found_in_mongo_collection(
    mocker, expected_list
):
    mock_hook = MagicMock()
    mock_collection = MagicMock()
    mock_collection.find_one.return_value = {SOURCES_KEY: expected_list}
    mock_hook.get_collection.return_value = mock_collection
    mocker.patch("utils.source_config.mongo.MongoHook", return_value=mock_hook)

    assert get_sources() == expected_list
