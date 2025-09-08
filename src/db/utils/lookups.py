# Import modules
import os
import json
from loguru import logger
from src.db.utils.parsers import clean_document, to_int
from src.config import RAW_COLLECTIONS_DIR


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


def resolve_creator(creator_id: str, creator_role, lookup_data) -> dict:
    """
    Resolves a creator by custom creator_id and returns:
    - _id: MongoDB ObjectId
    - {creator_role}_name: Full name (firstname + lastname)
    """
    doc = lookup_data["creators"].get(creator_id)
    if not doc:
        logger.warning(f"No creator found for ID '{creator_id}'")
        return {}
    full_name = f"{doc.get('creator_firstname', '').strip()} {doc.get('creator_lastname', '').strip()}"
    return {
        "_id": doc["_id"],
        f"{creator_role}_name": full_name
    }


def resolve_awards(match, lookup_data: dict) -> dict:
    """
    Resolves award subdocument from regex match groups.
    Omits award_category if category ID is 'ac001'.
    """
    award = resolve_lookup('awards', match.group(1), lookup_data)
    category = resolve_lookup('award_categories', match.group(2), lookup_data)
    status = resolve_lookup('award_statuses', match.group(4), lookup_data)

    if  match.group(2) == 'ac001':
        subdoc = {
            "_id": award["_id"],  # type: ignore
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


def resolve_format_entry(entry: str, lookup_data: dict) -> dict:
    """
    Parses a format string and resolves all known fields.
    Uses in-memory lookup_data for resolution.
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
        "release_date": fields.get("release_date"),
        "publisher": resolve_lookup("publishers", fields.get("publisher"), lookup_data),
        "language": fields.get("language"),
        "cover_art_id": resolve_lookup("cover_art", fields.get("cover_art"), lookup_data),
        "date_added": fields.get("date_added")
    }

    for role in ["translator", "narrator", "illustrator", "cover_artist", "editors"]:
        if role in fields:
            creator_ids = [cid.strip() for cid in fields[role].split(',')]
            singular_role = role[:-1] if role.endswith('s') else role
            doc[role] = [
                resolve_creator(cid, singular_role, lookup_data)
                for cid in creator_ids
            ]

    return clean_document(doc)
