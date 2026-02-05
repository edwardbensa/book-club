"""Collection cleanup"""

# Imports
import os
import json
from loguru import logger
from bson.objectid import ObjectId
from src.config import RAW_COLLECTIONS_DIR, TRANSFORMED_COLLECTIONS_DIR
from src.db.utils.transforms import remove_custom_ids, change_id_field, add_timestamp


# Collections to remove custom ids from
transformed_collections_to_cleanup = {
    "books": "book_id",
    "users": "user_id",
    "creators": "creator_id",
    "book_versions": "version_id",
}

raw_collections_to_cleanup = {
    "genres": "genre_id",
    "publishers": "publisher_id",
    "tags": "tag_id",
}

# Collections to modify id_fields to use custom string IDs
collections_to_modify = {
    "formats": "format_id",
    "languages": "language_id",
    "creator_roles": "cr_id",
    "read_statuses": "rstatus_id",
    "club_event_types": "event_type_id",
    "club_event_statuses": "event_status_id",
    "user_permissions": "permission_id",
    "countries": "country_id"
}

collections_to_timestamp = [
    "genres", "club_event_types", "club_event_statuses",
    "user_permissions", "publishers", "tags", "awards"
]

# Create deletions JSON
deletions = [{"_id": str(ObjectId()), "data": "placeholder"}]
deletions_path = os.path.join(TRANSFORMED_COLLECTIONS_DIR, "deletions.json")
with open(deletions_path, "w", encoding="utf-8") as f:
    json.dump(deletions, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    remove_custom_ids(raw_collections_to_cleanup, RAW_COLLECTIONS_DIR)
    remove_custom_ids(transformed_collections_to_cleanup, TRANSFORMED_COLLECTIONS_DIR)
    change_id_field(collections_to_modify, RAW_COLLECTIONS_DIR)
    for collection in collections_to_timestamp:
        add_timestamp(collection)
    logger.info("Cleaned collections.")
