# Import necessary modules
import os
import re
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

mongodb_uri = os.getenv("MONGODB_URI")
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


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
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string) # type: ignore
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


def get_name_mappings(db, name_field_map, name_field_lookup, collection_names):
    """
    Fetches custom IDs and their corresponding names from specified collections
    and returns a dictionary of mappings.
    """
    mappings = {}
    for name in collection_names:
        collection = db[name]
        try:
            id_field = name_field_map.get(name) or name + "_id"
            name_field = name_field_lookup.get(name, f"{name.rstrip('s')}_name")

            # Fetch all documents in a single query
            cursor = collection.find({}, {id_field: 1, name_field: 1})

            # Build the mapping in memory
            name_mapping = {
                doc[id_field]: doc[name_field]
                for doc in cursor
                if id_field in doc and name_field in doc
            }

            mappings[name] = name_mapping
            logger.info(f"Created name mapping for '{name}' collection.")
        except (ConnectionFailure, ConfigurationError) as e:
            logger.warning(f"Could not create name mapping for '{name}': {e}")
            mappings[name] = {}
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Unexpected error while creating name mapping for '{name}': {e}")
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


def change_id_field(db, collection_name, custom_id_field):
    """
    Replaces the MongoDB _id field with the value from custom_id_field.
    """
    try:
        collection = db[collection_name]
        for doc in collection.find():
            if custom_id_field in doc:
                new_id = str(doc[custom_id_field])
                # Prepare new document with new _id
                new_doc = doc.copy()
                new_doc["_id"] = new_id
                del new_doc[custom_id_field]

                try:
                    collection.insert_one(new_doc)
                    collection.delete_one({"_id": doc["_id"]})
                    logger.debug(f"Replaced _id of document {doc['_id']} with '{new_id}' in '{collection_name}'")
                except (KeyError, TypeError, ValueError) as insert_err:
                    logger.error(f"Failed to insert new document with _id '{new_id}': {insert_err}")
        logger.success(f"Object ID replacement completed for '{collection_name}' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to process collection '{collection_name}': {e}")


def make_blob_public(container_client, blob_name):
    """
    Sets the access level of a blob to public read.
    """
    try:
        acl = container_client.get_container_access_policy()
        if acl.get('public_access') != 'blob':
            container_client.set_container_access_policy(public_access='blob')
            logger.info(f"Set container access policy to public for blob '{blob_name}'.")
        else:
            logger.info(f"Blob '{blob_name}' is already public.")
    except (KeyError, TypeError, ValueError, AzureError) as e:
        logger.error(f"Failed to set blob '{blob_name}' to public: {e}")


def make_array_field(field_string):
    """
    Converts a comma-separated string into a list of trimmed strings.
    """
    if not field_string:
        return []
    return [item.strip() for item in field_string.split(',') if item.strip()]


def make_subdocuments(string, field_key, registry, separator=';'):
    """
    Parses a separator-separated string into a list of subdocuments
    using the pattern and transform function defined in the subdoc_registry.
    """
    if not string:
        return []

    config = registry.get(field_key)
    if not config:
        logger.error(f"No subdocument config found for field '{field_key}'")
        return []

    pattern = config['pattern']
    transform = config['transform']

    if not callable(transform):
        logger.error(f"Transform function missing or invalid for field '{field_key}'")
        raise ValueError(f"Config for field '{field_key}' must include a valid transform function.")

    doc_list = []
    for entry in string.split(separator):
        entry = entry.strip()
        if not entry:
            continue

        if pattern:
            match = pattern.match(entry)
            if match:
                doc_list.append(transform(match))
            else:
                logger.warning(f"No match for entry: '{entry.strip()}' in field '{field_key}'")
        else:
            doc_list.append(transform(entry))
    return doc_list


def fetch_collection_documents(db, collection_name):
    """
    Fetches all documents from the specified collection and returns them as a list.
    """
    try:
        collection = db[collection_name]
        documents = list(collection.find({}))
        logger.info(f"Fetched {len(documents)} documents from '{collection_name}' collection.")
        return documents
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch documents from '{collection_name}': {e}")
        return []


def replace_document(db, collection_name, old_id, new_doc):
    """
    Replaces a document in the specified collection with a new document.
    """
    try:
        collection = db[collection_name]
        collection.delete_one({"_id": old_id})
        collection.insert_one(new_doc)
        logger.info(f"Replaced document with _id '{old_id}' in '{collection_name}' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to replace document with _id '{old_id}' in '{collection_name}': {e}")


def replace_collection(db, collection_name, transformed_collection):
    """
    Replaces all documents in the specified collection with new documents.
    """
    if transformed_collection:
        # Drop exisiting collection and insert transformed collection
        db.drop_collection(collection_name)
        logger.info(f"Dropped existing '{collection_name}' collection.")

        db[collection_name].insert_many(transformed_collection)
        logger.info(f"Successfully imported {len(transformed_collection)} transformed documents into the '{collection_name}' collection.")
    else:
        logger.warning("No documents were transformed or imported into collection.")


def transform_collection(db, collection_name, transform_func):
    """
    Fetches documents from a collection, applies a transformation function,
    and replaces the original documents with the transformed ones.
    """
    documents = fetch_collection_documents(db, collection_name)
    transformed_collection = []
    for doc in documents:
        try:
            transformed_doc = transform_func(doc)
            transformed_collection.append(clean_document(transformed_doc))
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to transform document with _id '{doc.get('_id')}': {e}")
            continue
    replace_collection(db, collection_name, transformed_collection)


# PARSING AND CLEANING FUNCTIONS

def clean_document(doc):
    """
    Removes keys with None, empty lists, or empty strings from a document.
    """
    return {k: v for k, v in doc.items() if v is not None and v != [] and v != ''}


def to_datetime(date_string):
    """
    Converts a date string to a datetime object.
    Supports both 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM' formats.
    Returns None if the input is None, empty, or invalid.
    """
    if not date_string:
        return None

    formats = ['%Y-%m-%d %H:%M', '%Y-%m-%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except ValueError:
            continue

    logger.error(f"Invalid date format for '{date_string}'")
    return None


def to_int(value):
    """
    Converts a value to an integer. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError as e:
        logger.error(f"Invalid integer value '{value}': {e}")
        return None


def regex_extract(string, pattern='str, date'):
    """
    Extracts a substring from the input string that matches the given regex pattern.
    Returns None if no match is found or if the input string is None or empty.
    """
    if not string:
        return None

    if pattern == 'str, date':
        match = re.match(r'(.+),\s*(\d{4}-\d{2}-\d{2})', string.strip())
    elif pattern == 'key: value':
        match = re.compile(r'(\w+):\s*(\d+)')
    else:
        logger.error(f"Unsupported pattern '{pattern}'")
        return None
    return match if match else None
