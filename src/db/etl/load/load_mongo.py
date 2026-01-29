'''Loads transformed JSON collections into MongoDB'''

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

# Collections with ObjectIds in other fields
objectid_registry = {
    "books": ["series._id", "author._id", "contributors._id", "awards._id"],
    "book_versions": ["book_id", "publisher._id",],
    "club_members": ["club_id", "user_id"],
    "club_member_reads": ["club_id", "book_id", "user_id", "period_id"],
    "club_discussions": ["club_id", "comments.user_id", "created_by", "book_reference"],
    "club_events": ["created_by"],
    "club_reading_periods": ["club_id", "created_by"],
    "club_period_books": ["club_id", "book_id", "period_id"],
    "user_reads": ["book_id", "user_id", "version_id"],
    "clubs": ["created_by", "club_moderators"],
    "users": ["user_badges._id"],
}

def convert_fields(obj, collection_name: str, path=""):
    """
    Recursively converts:
    - _id fields to ObjectId unless collection_name is in custom_id_collections
    - Fields listed in objectid_registry[collection_name] to ObjectId
    - Any field with 'date', 'timestamp', or 'created_at' in its name to datetime
    """
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            full_path = f"{path}.{key}" if path else key

            if key == "_id" and path == "" and collection_name not in custom_id_collections:
                try:
                    new_obj["_id"] = ObjectId(value)
                except (TypeError, ValueError):
                    new_obj["_id"] = value
            elif any(tag in key.lower() for tag in ["date", "timestamp", "created_at"]):
                new_obj[key] = to_datetime(value)
            elif full_path in objectid_registry.get(collection_name, []):
                if isinstance(value, list):
                    new_obj[key] = [ObjectId(v) if isinstance(v, str) else v for v in value]
                else:
                    try:
                        new_obj[key] = ObjectId(value)
                    except (TypeError, ValueError):
                        new_obj[key] = value
            else:
                new_obj[key] = convert_fields(value, collection_name, full_path)
        return new_obj
    elif isinstance(obj, list):
        return [convert_fields(item, collection_name, path) for item in obj]
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
