"""Project config"""

# Imports
import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()
gsheet_cred = os.getenv("GSHEET_CRED")
mongodb_uri = os.getenv("MONGODB_URI")
azure_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
neo4j_uri = os.getenv("NEO4J_URI")
neo4j_user = os.getenv("NEO4J_USERNAME")
neo4j_pwd = os.getenv("NEO4J_PASSWORD")
key_registry_path = os.getenv("ENCRYPTION_KEYS")
hf_token = os.getenv("HUGGINGFACE_HUB_TOKEN")


# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
#logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

COVER_ART_DIR = EXTERNAL_DATA_DIR / "cover_art"
RAW_TABLES_DIR = RAW_DATA_DIR / "raw_tables"
RAW_COLLECTIONS_DIR = RAW_DATA_DIR / "raw_collections"
TRANSFORMED_COLLECTIONS_DIR = PROCESSED_DATA_DIR / "transformed_collections"
ETL_LOGS_DIR = PROJ_ROOT / "src" / "db" / "etl" / "logs"

MODELS_DIR = PROJ_ROOT / "models"

REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

SECRETS_DIR = PROJ_ROOT / "src" / "db" / "secrets"

# If tqdm is installed, configure loguru with tqdm.write
# https://github.com/Delgan/loguru/issues/135
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass
