"""Lookup utlity functions"""

# Import modules
import os
import json
from urllib.parse import urlparse
from loguru import logger
from src.config import RAW_COLLECTIONS_DIR
from .parsers import to_int
from .files import generate_image_filename
from .connectors import connect_azure_blob


# Load lookup collections from disk
def load_lookup_data(lookup_registry: dict) -> dict:
    """
    Loads JSON collections from disk and builds lookup maps
    based on the registry's field and get configuration.
    """
    lookup_data = {}

    for name, config in lookup_registry.items():
        path = os.path.join(RAW_COLLECTIONS_DIR, f"{name}.json")
        with open(path, encoding="utf-8") as f:
            collection = json.load(f)

        string_field = config["field"]
        get_fields = config["get"]

        if isinstance(get_fields, str):
            lookup_data[name] = {
                doc[string_field]: doc.get(get_fields)
                for doc in collection if string_field in doc
            }
        else:
            lookup_data[name] = {
                doc[string_field]: {field: doc.get(field) for field in get_fields}
                for doc in collection if string_field in doc
            }

    return lookup_data


def resolve_lookup(collection_name, input_string, lookup_data):
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
    return lookup_data.get(collection_name, {}).get(input_string)


def resolve_creator(creator_id: str, lookup_data) -> dict:
    """
    Resolves a creator by custom creator_id and returns:
    - _id: MongoDB ObjectId
    - {creator_role}_name: Full name (firstname + lastname)
    """
    doc = lookup_data["creators"].get(creator_id)
    if not doc:
        logger.warning(f"No creator found for ID '{creator_id}'")
        return {}
    full_name = f"{doc.get('firstname', '').strip()} {doc.get('lastname', '').strip()}"
    return {
        "_id": doc["_id"],
        "name": full_name
    }


def resolve_awards(match, lookup_data: dict) -> dict:
    """
    Resolves award subdocument from regex match groups.
    Omits award_category if category ID is ''.
    """

    if  match.group(3) == '':
        subdoc = {
            "_id": resolve_lookup('awards', match.group(1), lookup_data),
            "award_name": match.group(2),
            "year": to_int(match.group(4)),
            "award_status": match.group(5)
        }
    else:
        subdoc = {
                "_id": resolve_lookup('awards', match.group(1), lookup_data),
                "award_name": match.group(2),
                "award_category": match.group(3),
                "year": to_int(match.group(4)),
                "award_status": match.group(5)
            }

    return subdoc

blobserviceclient_account_name = connect_azure_blob().account_name

def generate_image_url(doc: dict, url_str: str, img_type: str, container_name: str) -> str:
    """
    Generates the Azure Blob Storage URL for a given document's image.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    try:
        if not url_str or not isinstance(url_str, str) or not url_str.strip():
            return ""
        parsed_url = urlparse(url_str)
        extension = os.path.splitext(parsed_url.path)[1] or ".jpg"

        blob_name = f"{generate_image_filename(doc, img_type)}{extension}"
        account_name = blobserviceclient_account_name
        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
        return url
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to generate image URL: {e}")
        return ""
