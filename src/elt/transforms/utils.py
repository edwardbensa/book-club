# Import necessary modules
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from src.secrets.secrets import mongodb_uri, azure_storage_connection_string


def connect_mongodb():
    """
    Connects to the MongoDB database and returns the database object.
    """
    try:
        client = MongoClient(mongodb_uri)
        db = client["book_club"]
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return db, client
    except (ConnectionFailure, ConfigurationError)  as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        exit()

def connect_azure_blob():
    """
    Connects to Azure Blob Storage and returns the BlobServiceClient.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        logger.info("Successfully connected to Azure Blob Storage.")
        return blob_service_client
    except (ConnectionFailure, ConfigurationError) as e:
        logger.error(f"Failed to connect to Azure Blob Storage: {e}")
        exit()

def get_id_mappings(db, id_field_map, collection_names):
    """
    Fetches custom IDs and MongoDB ObjectIds from specified collections
    and returns a dictionary of mappings.
    """
    mappings = {}
    for name in collection_names:
        collection = db[name]
        try:
            # Use the provided id_field_map to determine the field name
            id_field = id_field_map.get(name) or name + "_id"

            cursor = collection.find({}, {id_field: 1, '_id': 1})

            mappings[name] = {doc[id_field]: doc['_id'] for doc in cursor if id_field in doc}
            logger.info(f"Created mapping for '{name}' collection.")
        except (ConnectionFailure, ConfigurationError) as e:
            logger.warning(f"Could not create mapping for '{name}': {e}")
            mappings[name] = {}
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Unexpected error while creating mapping for '{name}': {e}")
            mappings[name] = {}
    return mappings


def parse_multi_value_field(id_mappings, field_string, collection_name):
    """
    Parses a comma-separated string of IDs and converts them into a list of
    MongoDB ObjectIds using the actual mapping dictionaries from the database.
    """
    if not field_string:
        return []

    ids = [item.strip() for item in field_string.split(',')]
    object_ids = [
        id_mappings[collection_name].get(old_id)
        for old_id in ids if old_id in id_mappings[collection_name]
    ]
    return object_ids

def remove_custom_ids(db, collection_name, custom_id_field):
    """
    Removes the specified custom ID field from all documents in the given collection.
    """
    try:
        db[collection_name].update_many(
            {},  # An empty filter means update all documents
            {"$unset": {custom_id_field: ""}}
        )
        logger.success(f"Removed '{custom_id_field}' field from all documents in '{collection_name}'.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to remove '{custom_id_field}' field from '{collection_name}': {e}")
