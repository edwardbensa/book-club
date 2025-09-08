# Import modules
import re
from src.db.utils.parsers import to_datetime, to_int
from src.db.utils.doc_transformers import transform_collection, make_subdocuments, resolve_lookup
from src.db.utils.security import encrypt_pii, hash_password, latest_key_version


# Define field lookups
lookup_registry = {
    'books': {
        'string_field': 'book_id',
        'get': '_id'
    },
    'genres': {
        'string_field': 'genre_name',
        'get': ['_id', 'genre_name']
    },
    'users': {
        'string_field': 'user_id',
        'get': '_id'
    },
    'user_badges': {
        'string_field': 'badge_name',
        'get': ['_id', 'badge_name']
    },
    'read_statuses': {
        'string_field': 'rstatus_id',
        'get': 'rstatus_name'
    }
}

subdoc_registry = {
    'rstatus_history': {
        'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2})'),
        'transform': lambda match: {
            "rstatus": resolve_lookup('read_statuses', match.group(1), lookup_registry),
            "timestamp": to_datetime(match.group(2))
        }
    },
    'user_readinggoal': {
        'pattern': re.compile(r'year:\s*(\d+),\s*goal:\s*(\d+)'),
        'transform': lambda match: {
            "year": to_int(match.group(1)),
            "goal": to_int(match.group(2))
        }
    },
    'user_badges': {
        'pattern': re.compile(r'badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})'),
        'transform': lambda match: {
            **resolve_lookup('user_badges', match.group(1), lookup_registry), # type: ignore
            "timestamp": to_datetime(match.group(2))
        }
    },
    'user_genres': {
        'pattern': None,
        'transform': lambda genre_name: resolve_lookup('genres', genre_name, lookup_registry)
    }
}


def transform_user_reads_func(doc):
    """
    Transforms a user_reads document to the desired structure.
    """
    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": resolve_lookup('users', doc.get("user_id"), lookup_registry),
        "book_id": resolve_lookup('books', doc.get("book_id"), lookup_registry),
        "current_rstatus": resolve_lookup('read_statuses', doc.get("current_rstatus_id"),
                                          lookup_registry),
        "rstatus_history": make_subdocuments(doc.get("rstatus_history"), 'rstatus_history',
                                             subdoc_registry, separator=','),
        "date_started": to_datetime(doc.get("date_started")),
        "date_completed": to_datetime(doc.get("date_completed")),
        "book_rating": None if doc.get("book_rating") == "" else int(doc.get("book_rating")),
        "notes": doc.get("notes"),
    }
    return transformed_doc


def transform_users_func(doc):
    """
    Transforms a user document to the desired structure.
    """
    key_version = latest_key_version

    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": doc.get("user_id"),
        "user_handle": doc.get("user_handle"),
        "user_firstname": doc.get("user_firstname"),
        "user_lastname": doc.get("user_lastname"),
        "user_emailaddress": encrypt_pii(doc.get("user_emailaddress"), version=key_version),
        "user_password": hash_password(doc.get("user_password")),
        "user_dob": encrypt_pii(doc.get("user_dob"), version=key_version),
        "user_gender": encrypt_pii(doc.get("user_gender"), version=key_version),
        "user_city": encrypt_pii(doc.get("user_city"), version=key_version),
        "user_state": encrypt_pii(doc.get("user_state"), version=key_version),
        "user_country": encrypt_pii(doc.get("user_country"), version=key_version),
        "user_bio": doc.get("user_bio"),
        "user_readinggoal": make_subdocuments(doc.get("user_readinggoal"), 'user_readinggoal',
                                             subdoc_registry, separator='|'),
        "user_badges": make_subdocuments(doc.get("user_badges"), 'user_badges',
                                             subdoc_registry, separator='|'),
        "user_genres": make_subdocuments(doc.get("user_genres"), 'user_genres',
                                             subdoc_registry, separator=','),
        "date_joined": to_datetime(doc.get("date_joined")),
        "key_version": key_version
    }
    return transformed_doc


# Transform 'user_reads' collection
if __name__ == "__main__":
    transform_collection("user_reads", transform_user_reads_func)
    transform_collection("users", transform_users_func)
