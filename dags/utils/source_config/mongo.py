import typing

from airflow.providers.mongo.hooks.mongo import MongoHook

SOURCES_MONGO_DB_CONN_ID = "sources_mongo_db"
SOURCES_MONGO_DB = "local"
SOURCES_MONGO_COLLECTION = "configs"

SOURCES_CONFIG_NAME = "sources"
SOURCES_KEY = "sources"


def get_sources() -> typing.List[str]:
    hook = MongoHook(conn_id=SOURCES_MONGO_DB_CONN_ID)
    config_collection = hook.get_collection(
        mongo_db=SOURCES_MONGO_DB, mongo_collection=SOURCES_MONGO_COLLECTION
    )

    sources_config = config_collection.find_one(
        filter={"name": SOURCES_CONFIG_NAME},
        sort=[("$natural", -1)],
    )
    if not sources_config:
        return []

    return sources_config.get(SOURCES_KEY, [])
