"""Routine MongoDB operations"""

# Imports
from datetime import datetime
from loguru import logger


def archive_delete(db, collection_name, filter_query):
    """Archive doc in deletions collection and delete from original."""
    source = db[collection_name]
    deletions = db["deletions"]

    # Find document
    doc = source.find_one(filter_query)
    if not doc:
        return {"deleted": False, "reason": "Document not found"}

    # Prepare archived version
    archived_doc = {
        **doc,
        "original_collection": collection_name,
        "deleted_at": datetime.now()
    }

    # Insert into deletions collection
    deletions.insert_one(archived_doc)

    # Delete from original collection
    result = source.delete_one({"_id": doc["_id"]})

    return {
        "deleted": result.deleted_count == 1,
        "archived": True,
        "id": str(doc["_id"])
    }

def drop_all_collections(db):
    """Drop all existing collections"""
    for name in db.list_collection_names():
        db.drop_collection(name)
        logger.info(f"Dropped collection '{name}'")
