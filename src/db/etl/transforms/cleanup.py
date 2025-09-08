# Import modules
from loguru import logger
from src.db.utils.connectors import connect_mongodb
from src.db.utils.doc_transformers import remove_custom_ids, change_id_field

# Connect to MongoDB
db, client = connect_mongodb()

# Collections to clean up
collections_to_cleanup = {
    "book_collections": "bcollection_id",
    "award_categories": "acategory_id",
    "award_statuses": "astatus_id",
    "cover_art": "cart_id",
    "genres": "genre_id",
    "publishers": "publisher_id",
    "tags": "tag_id",
    "users": "user_id",
}

for collection_name, id_field in collections_to_cleanup.items():
    collection = db[collection_name]
    logger.info(f"Starting cleanup for '{collection_name}' collection...")

    # Remove custom IDs from the collection
    remove_custom_ids(db, collection_name, id_field)

    # Log the completion of the cleanup
    logger.success(f"Cleanup completed for '{collection_name}' collection.")

# Modify id_fields to use custom string IDs
collections_to_modify = {
    "formats": "format_id",
    "languages": "language_id",
    "creator_roles": "cr_id",
    "read_statuses": "rstatus_id",
    "club_event_types": "event_type_id",
    "club_event_statuses": "event_status_id",
    "club_member_roles": "role_id",
}

for collection_name, id_field in collections_to_modify.items():
    collection = db[collection_name]
    logger.info(f"Modifying IDs for '{collection_name}' collection...")

    # Update documents to use custom string IDs
    change_id_field(db, collection_name, id_field)

    # Log the completion of the modification
    logger.success(f"ID modification completed for '{collection_name}' collection.")


# Cleanup for the 'award_categories' collection
award_categories_collection = db["award_categories"]
logger.info("Starting cleanup for 'award_categories' collection...")

# Find and remove the first entry.
first_entry = award_categories_collection.find_one()

if first_entry:
    entry_id_to_remove = first_entry['_id']
    logger.info(f"Found first entry to remove with _id: {entry_id_to_remove}")

    # Remove the identified document.
    result = award_categories_collection.delete_one({"_id": entry_id_to_remove})

    if result.deleted_count == 1:
        logger.success(f"Successfully removed the first entry with _id: {entry_id_to_remove}")
    else:
        logger.warning(f"Failed to remove the first entry. Deleted count was {result.deleted_count}.")
else:
    logger.warning("No entries found in 'award_categories' collection to remove.")

client.close()
logger.info("MongoDB connection closed.")
