from pathlib import Path

# Environments
ENV = "ENV"
TEST_ENV = "test"
DEFAULT_ENV_AS_TEST = TEST_ENV

# Configuration
DAG_DIR = Path(__file__).parent
CONFIG_DIR = "configs"

SOURCES_FILE_NAME = "sources.yaml"
SOURCE_CONFIG_FILE_PATH = DAG_DIR / CONFIG_DIR / SOURCES_FILE_NAME
