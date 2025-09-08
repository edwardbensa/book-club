# Import modules
import json
from pathlib import Path
from bson import ObjectId
from loguru import logger
from src.config import TRANSFORMED_COLLECTIONS_DIR
from src.db.utils.connectors import connect_mongodb
from src.db.utils.parsers import to_datetime
from src.db.etl.transforms.cleanup import collections_to_modify

# Connect to MongoDB
db, client = connect_mongodb()

# Drop all existing collections
for name in db.list_collection_names():
    db.drop_collection(name)
    logger.info(f"Dropped collection '{name}'")

# Collections that use custom string-based _id fields
custom_id_collections = list(collections_to_modify.keys())

def convert_fields(obj, collection_name: str):
    """
    Recursively converts:
    - '_id' fields to ObjectId unless collection_name is in custom_id_collections
    - Any field with 'date', 'timestamp', or 'created_at' in its name to datetime
    """
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            if key == "_id":
                if collection_name in custom_id_collections:
                    new_obj["_id"] = value  # leave as-is
                else:
                    try:
                        new_obj["_id"] = ObjectId(value)
                    except (KeyError, TypeError, ValueError):
                        new_obj["_id"] = value
            elif any(tag in key.lower() for tag in ["date", "timestamp", "created_at"]):
                new_obj[key] = to_datetime(value)
            else:
                new_obj[key] = convert_fields(value, collection_name)
        return new_obj
    elif isinstance(obj, list):
        return [convert_fields(item, collection_name) for item in obj]
    else:
        return obj

def load_transformed_collections():
    """
    Loads all transformed JSON collections from TRANSFORMED_COLLECTIONS_DIR
    into MongoDB, converting _id and datetime fields appropriately.
    """
    directory = Path(TRANSFORMED_COLLECTIONS_DIR)
    json_files = list(directory.glob("*.json"))

    if not json_files:
        logger.warning("No JSON files found in TRANSFORMED_COLLECTIONS_DIR.")
        return

    for file_path in json_files:
        collection_name = file_path.stem
        try:
            with file_path.open(encoding="utf-8") as f:
                raw_docs = json.load(f)
            logger.info(f"Loaded {len(raw_docs)} documents from '{file_path.name}'")

            cleaned_docs = [convert_fields(doc, collection_name) for doc in raw_docs]

            db.drop_collection(collection_name)
            db[collection_name].insert_many(cleaned_docs)
            logger.success(f"Inserted {len(cleaned_docs)} documents into MongoDB collection '{collection_name}'")

        except (KeyError, TypeError, ValueError) as e:
            logger.error(f"Failed to load collection '{collection_name}': {e}")

    client.close()
    logger.info("MongoDB connection closed.")

# Run the loader
if __name__ == "__main__":
    load_transformed_collections()
