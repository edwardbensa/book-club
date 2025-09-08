# Import modules
import os
import csv
from typing import Any, Dict, Optional, Union, List
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from dotenv import load_dotenv
from src.db.utils.parsers import clean_document, to_datetime, to_int
from src.db.utils.connectors import connect_mongodb
from src.config import RAW_TABLES_DIR

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

# Connect MongoDB
db, client = connect_mongodb()


def refresh_collection(collection_name):
    """
    Drops the specified collection and reloads it from a CSV file
    named <collection_name>.csv located in the given directory.
    """
    csv_path = os.path.join(RAW_TABLES_DIR, f"{collection_name}.csv")

    # Drop the collection
    db.drop_collection(collection_name)
    logger.info(f"Dropped collection '{collection_name}'")

    # Load CSV
    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            documents = [dict(row) for row in reader]
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        return
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Error reading CSV file '{csv_path}': {e}")
        return

    # Insert into MongoDB
    if documents:
        db[collection_name].insert_many(documents)
        logger.info(f"Inserted {len(documents)} documents into '{collection_name}'")
    else:
        logger.warning(f"No documents found in CSV '{csv_path}'")


def get_id_mappings(id_field_map, collection_names):
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


def get_name_mappings(name_field_map, name_field_lookup, collection_names):
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


def remove_custom_ids(collection_name, custom_id_field):
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


def change_id_field(collection_name, custom_id_field):
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


def make_array_field(field_string):
    """
    Converts a comma-separated string into a list of trimmed strings.
    """
    if not field_string:
        return []
    return [item.strip() for item in field_string.split(',') if item.strip()]



def make_subdocuments(string: str, field_key: str, registry: Dict[str, Any], separator: str = ';') -> List[Dict[str, Any]]:
    """
    Parses a separator-separated string into a list of subdocuments
    using the pattern and transform function defined in the subdoc_registry.
    """
    if not string:
        return []

    config = registry.get(field_key)
    if not config or not callable(config.get('transform')):
        logger.error(f"Invalid subdocument config for field '{field_key}'")
        return []

    pattern = config.get('pattern')
    transform = config['transform']

    entries = [entry.strip() for entry in string.split(separator) if entry.strip()]

    if pattern:
        # Use a list comprehension to process entries with a pattern
        transformed_list = []
        for entry in entries:
            match = pattern.match(entry)
            if match:
                transformed_list.append(transform(match))
            else:
                logger.warning(f"No match for entry: '{entry}' in field '{field_key}'")
        return transformed_list
    else:
        # Use a list comprehension for a more concise and faster loop
        return [transform(entry) for entry in entries]


# - LOOKUPS -

def resolve_lookup(
    collection_name: str, input_string: str, registry: Dict[str, Any]
) -> Optional[Union[Dict[str, Any], Any]]:
    """
    Uses a lookup registry to return specified fields.

    Args:
        collection_name: The name of the collection to search.
        input_string: The value to search for.
        registry: The lookup configuration registry.

    Returns:
        If 'get' is a string, returns a single value.
        If 'get' is a list, returns a dictionary of values.
        Returns None if no match is found or configuration is incomplete.
    """
    if not input_string:
        return None

    config = registry.get(collection_name)
    if not config:
        logger.warning(f"No lookup config found for collection '{collection_name}'")
        return None

    string_field = config.get('string_field')
    get_fields = config.get('get')

    if not string_field or not get_fields:
        logger.warning(f"Incomplete lookup config for '{collection_name}'")
        return None

    # Determine projection and single_field flag
    if isinstance(get_fields, list):
        projection = {field: 1 for field in get_fields}
        is_single_field = False
    else:  # Assumes get_fields is a string
        projection = {get_fields: 1}
        is_single_field = True
        get_fields = [get_fields]  # Convert to a list for unified processing

    doc = db[collection_name].find_one({string_field: input_string}, projection)

    if not doc:
        logger.warning(f"No match found in '{collection_name}' for '{input_string}'")
        return None

    if is_single_field:
        return doc.get(get_fields[0])
    else:
        return {field: doc.get(field) for field in get_fields}


def resolve_creator(creator_id: str, creator_role='author') -> dict:
    """
    Resolves a creator by custom creator_id and returns:
    - {creator_role}_id: MongoDB ObjectId
    - {creator_role}_name: Full name (firstname + lastname)
    """
    doc = db["creators"].find_one(
        {"creator_id": creator_id},
        {"_id": 1, "creator_firstname": 1, "creator_lastname": 1}
    )

    if not doc:
        logger.warning(f"No creator found for ID '{creator_id}'")
        return {}

    full_name = f"{doc.get('creator_firstname', '').strip()
                   } {doc.get('creator_lastname', '').strip()}"

    return {
        "_id": doc["_id"],
        f"{creator_role}_name": full_name
    }


def resolve_awards(match, lookup_registry: dict) -> dict:
    """
    Resolves award subdocument from regex match groups.
    Omits award_category if category ID is 'ac001'.
    """
    award = resolve_lookup('awards', match.group(1), lookup_registry)
    category = resolve_lookup('award_categories', match.group(2), lookup_registry)
    status = resolve_lookup('award_statuses', match.group(4), lookup_registry)

    if  match.group(2) == 'ac001':
        subdoc = {
            "award_id": award["_id"],  # type: ignore
            "award_name": award["award_name"],  # type: ignore
            "year": to_int(match.group(3)),
            "award_status": status
        }
    else:
        subdoc = {
                "award_id": award["_id"],  # type: ignore
                "award_name": award["award_name"],  # type: ignore
                "award_category": category,
                "year": to_int(match.group(3)),
                "award_status": status
            }

    return subdoc


def resolve_format_entry(entry: str, lookup_registry: dict) -> dict:
    """
    Parses a format string and resolves all known fields.
    Handles optional and multi-value creator roles.
    """
    fields = {}
    for part in entry.split(';'):
        if ':' not in part:
            continue
        key, value = part.strip().split(':', 1)
        fields[key.strip()] = value.strip()

    doc = {
        "format": fields.get("format"),
        "edition": fields.get("edition"),
        "isbn_13": fields.get("isbn_13"),
        "asin": fields.get("asin"),
        "page_count": to_int(fields.get("page_count")),
        "length": fields.get("length"),
        "release_date": to_datetime(fields.get("release_date")),
        "publisher": resolve_lookup('publishers',
                                    fields.get("publisher"), lookup_registry), # type: ignore
        "language": fields.get("language"),
        "cover_art_id": resolve_lookup('cover_art',
                                       fields.get("cover_art"), lookup_registry), # type: ignore
        "created_on": to_datetime(fields.get("created_on"))
    }

    for role in ["translator", "narrator", "illustrator", "cover_artist", "editors"]:
        if role in fields:
            creator_ids = [cid.strip() for cid in fields[role].split(',')]
            singular_role = role[:-1] if role.endswith('s') else role
            doc[role] = [
                resolve_creator(cid, creator_role=singular_role)
                for cid in creator_ids
            ]

    return clean_document(doc)


# - DOCUMENT TRANSFORMATION -

def transform_collection(collection_name, transform_func):
    """
    Fetches documents from a collection, applies a transformation function,
    and replaces the original documents with the transformed ones in a single operation.
    """
    try:
        # Step 1: Fetch documents from the specified collection
        collection = db[collection_name]
        documents = list(collection.find({}))
        if not documents:
            logger.warning(f"No documents found in '{collection_name}' to transform.")
            return

        logger.info(f"Fetched {len(documents)} documents from '{collection_name}' collection.")

        # Step 2: Transform documents
        transformed_docs = []
        for doc in documents:
            try:
                # Apply the transformation function and clean the resulting document
                transformed_doc = transform_func(doc)
                transformed_docs.append(clean_document(transformed_doc))
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"Failed to transform document with _id '{doc.get('_id')}': {e}")
                continue

        if not transformed_docs:
            logger.warning(f"No documents were successfully transformed for '{collection_name}'.")
            return

        # Step 3: Replace the collection
        # Drop the existing collection
        db.drop_collection(collection_name)
        logger.info(f"Dropped existing '{collection_name}' collection.")

        # Insert the newly transformed documents
        db[collection_name].insert_many(transformed_docs)
        logger.info(f"Successfully imported {len(transformed_docs)} transformed documents into the '{collection_name}' collection.")

    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"An error occurred during transformation and replacement of '{collection_name}': {e}")
