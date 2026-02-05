"""Loads transformed JSON collections into MongoDB"""

# Import modules
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bson import ObjectId
from loguru import logger
from pymongo import UpdateOne
from src.config import TRANSFORMED_COLLECTIONS_DIR
from src.db.utils.connectors import connect_mongodb
from src.db.utils.parsers import to_datetime
from src.db.etl.transforms.cleanup import collections_to_modify

# Connect to MongoDB
db, client = connect_mongodb()

# Collections that use custom string-based _id fields
collections_to_modify["user_roles"] = ""
custom_id_collections = list(collections_to_modify.keys())

# Collections with ObjectIds in other fields
objectid_registry = {
    "books": ["series._id", "author._id", "contributors._id", "awards._id"],
    "book_versions": ["book_id", "publisher._id"],
    "club_members": ["club_id", "user_id"],
    "club_member_reads": ["club_id", "book_id", "user_id", "period_id"],
    "club_discussions": ["club_id", "comments.user_id", "created_by", "book_reference"],
    "club_events": ["created_by"],
    "club_reading_periods": ["club_id", "created_by"],
    "club_period_books": ["club_id", "book_id", "period_id", "votes.user_id"],
    "user_reads": ["book_id", "user_id", "version_id"],
    "clubs": ["created_by", "moderators", "badges._id"],
    "users": ["user_badges._id"],
}

timestamp = datetime.now()

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

    if isinstance(obj, list):
        return [convert_fields(item, collection_name, path) for item in obj]

    return obj

def load_single_collection(file_path):
    """
    Load single transformed collection into MongoDB,
    converting _id and datetime fields appropriately.
    """
    collection_name = file_path.stem
    try:
        with file_path.open(encoding="utf-8") as f:
            raw_docs = json.load(f)

        cleaned_docs = [convert_fields(doc, collection_name) for doc in raw_docs]
        cleaned_docs = [{**doc, "updated_at": timestamp} for doc in cleaned_docs] # type: ignore
        collection = db[collection_name]

        ops = [
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) # type: ignore
            for doc in cleaned_docs
        ]

        if ops:
            result = collection.bulk_write(ops, ordered=False)
            logger.success(
                f"{collection_name}: {result.upserted_count} added, "
                f"{result.modified_count} updated."
            )

    except (KeyError, TypeError, ValueError, FileNotFoundError) as e:
        logger.error(f"Failed to load '{collection_name}': {e}")

def load_collections():
    """Load collections in parallel."""
    directory = Path(TRANSFORMED_COLLECTIONS_DIR)
    json_files = list(directory.glob("*.json"))

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(load_single_collection, fp) for fp in json_files]

        for future in as_completed(futures):
            future.result()  # triggers exceptions if any

    client.close()
    logger.info("MongoDB connection closed.")



# Run
if __name__ == "__main__":
    load_collections()
