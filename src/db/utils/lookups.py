import os
import json
from loguru import logger
from src.db.utils.parsers import clean_document, to_int
from src.config import RAW_COLLECTIONS_DIR, TRANSFORMED_COLLECTIONS_DIR


# Load lookup collections from disk
def build_lookup_map(name: str, string_field: str, get_fields):
    """
    Loads a JSON collection from disk and builds a lookup map.
    - name: collection name (e.g. 'creators')
    - string_field: field to use as lookup key (e.g. 'creator_id')
    - get_fields: field(s) to retrieve (str or list)
    """
    path = os.path.join(RAW_COLLECTIONS_DIR, f"{name}.json")
    with open(path, encoding="utf-8") as f:
        collection = json.load(f)

    if isinstance(get_fields, str):
        return {doc[string_field]: doc.get(get_fields) for doc in collection}
    else:
        return {
            doc[string_field]: {field: doc.get(field) for field in get_fields}
            for doc in collection
        }


def resolve_lookup(collection_name, input_string, lookup_data):
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
        "created_on": fields.get("created_on")
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


def transform_collection(collection_name: str, transform_func):
    """
    Loads a raw JSON collection, transforms each document,
    and writes the result to TRANSFORMED_COLLECTIONS_DIR.
    Assumes _id is already present in the input.
    """
    input_path = os.path.join(RAW_COLLECTIONS_DIR, f"{collection_name}.json")
    output_path = os.path.join(TRANSFORMED_COLLECTIONS_DIR, f"{collection_name}.json")

    try:
        with open(input_path, encoding="utf-8") as f:
            raw_docs = json.load(f)

        transformed = []
        for doc in raw_docs:
            transformed.append(clean_document(transform_func(doc)))

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)

        logger.info(f"Transformed {len(transformed)} records → {output_path}")

    except FileNotFoundError:
        logger.warning(f"Raw JSON file not found: {input_path}")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Error transforming '{collection_name}': {e}")
