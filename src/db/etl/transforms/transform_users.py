"""Transform users"""

# Import modules
import re
import json
from datetime import datetime
from src.db.utils.transforms import transform_collection, add_read_details
from src.db.utils.parsers import to_int, make_subdocuments, to_array
from src.db.utils.lookups import resolve_lookup, load_lookup_data
from src.db.utils.security import encrypt_pii, hash_password, latest_key_version
from src.config import RAW_COLLECTIONS_DIR


# Define field lookups
lookup_registry = {
    'book_versions': {'field': 'version_id', 'get': '_id'},
    'genres': {'field': 'genre_name', 'get': ['_id', 'name']},
    'users': {'field': 'user_id', 'get': '_id'},
    'user_badges': {'field': 'name', 'get': ['_id', 'name']},
    'read_statuses': {'field': 'rstatus_id', 'get': 'name'},
    'clubs': {'field': 'club_id', 'get': '_id'},
}

lookup_data = load_lookup_data(lookup_registry)

subdoc_registry = {
    'reading_log': {
        'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)'),
        'transform': lambda match: {
            "rstatus": resolve_lookup('read_statuses', match.group(1), lookup_data),
            "timestamp": match.group(2)
        }
    },
    'reading_goal': {
        'pattern': re.compile(r'year:\s*(\d+),\s*goal:\s*(\d+)'),
        'transform': lambda match: {
            "year": to_int(match.group(1)),
            "goal": to_int(match.group(2))
        }
    },
    'badges': {
        'pattern': re.compile(r'badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})'),
        'transform': lambda match: {
            **resolve_lookup('user_badges', match.group(1), lookup_data), # type: ignore
            "timestamp": match.group(2)
        }
    },
    'preferred_genres': {
        'pattern': None,
        'transform': lambda genre_name: resolve_lookup('genres', genre_name, lookup_data)
    },
    "clubs": {
        "pattern": re.compile(r"_id:\s*(\w+),\s*role:\s*(\w+),\s*joined:\s*(\d{4}-\d{2}-\d{2})"),
        "transform": lambda match: {
            "_id": lookup_data["clubs"].get(match.group(1)),
            "role": match.group(2)
        }
    }
}

# Load book_versions
with open(RAW_COLLECTIONS_DIR / "book_versions.json", "r", encoding="utf-8") as f:
    book_versions = json.load(f)

def transform_user_reads_func(doc):
    """
    Transforms a user_reads document to the desired structure.
    """

    # Modify doc
    doc = add_read_details(doc, book_versions)

    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": resolve_lookup('users', doc.get("user_id"), lookup_data),
        "version_id": resolve_lookup('book_versions', doc.get("version_id"), lookup_data),
        "rstatus": resolve_lookup('read_statuses', doc.get("rstatus_id"), lookup_data),
        "reading_log": make_subdocuments(doc.get("reading_log"), 'reading_log',
                                             subdoc_registry, separator=','),
        "date_started": doc.get("date_started"),
        "date_completed": doc.get("date_completed"),
        "days_to_read": doc.get("days_to_read"),
        "pages_per_day": doc.get("pages_per_day"),
        "hours_per_day": doc.get("hours_per_day"),
        "rating": None if doc.get("rating") == "" else int(doc.get("rating")),
        "notes": doc.get("notes"),
    }
    return transformed_doc


def transform_user_roles_func(doc):
    """
    Transforms a user_roles document to the desired structure.
    """
    return {
        "_id": doc.get("role_id"),
        "name": doc.get("name"),
        "permissions": to_array(doc.get("permissions")),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }

def transform_user_badges_func(doc):
    """
    Transforms a user_badges document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }


def transform_users_func(doc):
    """
    Transforms a user document to the desired structure.
    """
    key_version = latest_key_version

    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": doc.get("user_id"),
        "handle": doc.get("handle"),
        "firstname": doc.get("firstname"),
        "lastname": doc.get("lastname"),
        "email_address": encrypt_pii(doc.get("email_address"), version=key_version),
        "password": hash_password(doc.get("password")),
        "dob": encrypt_pii(doc.get("dob"), version=key_version),
        "gender": encrypt_pii(doc.get("gender"), version=key_version),
        "city": encrypt_pii(doc.get("city"), version=key_version),
        "state": encrypt_pii(doc.get("state"), version=key_version),
        "country": encrypt_pii(doc.get("country"), version=key_version),
        "bio": doc.get("bio"),
        "reading_goal": make_subdocuments(doc.get("reading_goal"), 'reading_goal',
                                             subdoc_registry, separator='|'),
        "badges": make_subdocuments(doc.get("badges"), 'badges',
                                             subdoc_registry, separator='|'),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "forbidden_genres": to_array(doc.get("forbidden_genres")),
        "clubs": make_subdocuments(doc.get("clubs"), 'clubs',
                                        subdoc_registry, separator='|'),
        "date_joined": doc.get("date_joined"),
        "last_active_date": doc.get("last_active_date"),
        "is_admin": bool(doc.get("is_admin", False)),
        "key_version": key_version
    }
    return transformed_doc


# Transform 'user_reads' collection
if __name__ == "__main__":
    transform_collection("user_reads", transform_user_reads_func)
    transform_collection("user_roles", transform_user_roles_func)
    transform_collection("user_badges", transform_user_badges_func)
    transform_collection("users", transform_users_func)
