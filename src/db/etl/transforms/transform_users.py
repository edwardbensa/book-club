"""Transform users"""

# Import modules
import re
import json
from datetime import datetime
from src.db.utils.transforms import transform_collection
from src.db.utils.parsers import to_int, make_subdocuments, to_array, to_datetime
from src.db.utils.lookups import resolve_lookup, load_lookup_data
from src.db.utils.security import encrypt_pii, hash_password, latest_key_version
from src.config import TRANSFORMED_COLLECTIONS_DIR, RAW_COLLECTIONS_DIR


# Define field lookups
lookup_registry = {
    'book_versions': {'field': 'version_id', 'get': '_id'},
    'genres': {'field': 'genre_name', 'get': ['_id', 'genre_name']},
    'users': {'field': 'user_id', 'get': '_id'},
    'user_badges': {'field': 'badge_name', 'get': ['_id', 'badge_name']},
    'read_statuses': {'field': 'rstatus_id', 'get': 'rstatus_name'},
    'clubs': {'field': 'club_id', 'get': '_id'},
}

lookup_data = load_lookup_data(lookup_registry)

subdoc_registry = {
    'rstatus_history': {
        'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2})'),
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
        "pattern": re.compile(r"_id:\s*(\w+),\s*role:\s*(\w+)"),
        "transform": lambda match: {
            "_id": lookup_data["clubs"].get(match.group(1)),
            "role": match.group(2)
        }
    }
}

def add_dtc(user_reads):
    """Calculate the time to completion and add to user_reads."""

    for doc in user_reads:
        s_date = doc.get("date_started", None)
        e_date = doc.get("date_completed", datetime.now())
        e_date = to_datetime(e_date) if isinstance(e_date, str) else e_date
        if s_date is None:
            continue

        events = doc.get("rstatus_history", [])
        events.append({"rstatus": "Reading", "timestamp": s_date})
        events = [{"rstatus": i["rstatus"], "timestamp": to_datetime(i["timestamp"])}
                   for i in events]

        events = sorted(events, key=lambda x: x["timestamp"])
        events.append({"rstatus": "Read", "timestamp": e_date})
        if events[-2]["rstatus"] == "DNF":
            events[-2]["rstatus"] = "Read"
            events = events[:-1]

        total = datetime.min - datetime.min  # zero timedelta
        current_start = None

        for status, ts in [(e["rstatus"], e["timestamp"]) for e in events]:
            if status == "Reading":
                # start (or restart) a reading interval
                current_start = ts
            elif status in ("Paused", "Read") and current_start is not None:
                # close current reading interval
                total += ts - current_start
                current_start = None

            if status == "Read":
                break

        dtc = total.total_seconds() / 86400
        dtc = 1 if dtc == 0 else dtc
        doc["days_to_read"] = dtc

    return user_reads

def add_reading_rates(user_reads, book_versions):
    """Calculate reading rates and add to user_reads."""

    user_reads = add_dtc(user_reads)

    for doc in user_reads:
        version_id = doc.get("version_id")
        dtc = doc.get("days_to_read", None)

        if dtc is None:
            continue

        fmt = str([i.get("format") for i in book_versions if i["_id"] == version_id][0])

        if fmt == "":
            continue

        if fmt == "audiobook":
            length = str([i.get("length") for i in book_versions if i["_id"] == version_id][0])
            if length == "":
                continue
            hpd = float(length) / dtc
            doc["read_rate_hours"] = hpd
        else:
            pc = str([i.get("page_count") for i in book_versions if i["_id"] == version_id][0])
            if pc == "":
                continue
            ppd = float(pc) / dtc
            doc["read_rate_pages"] = ppd

    return user_reads

def transform_user_reads_func(doc):
    """
    Transforms a user_reads document to the desired structure.
    """
    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": resolve_lookup('users', doc.get("user_id"), lookup_data),
        "version_id": resolve_lookup('book_versions', doc.get("version_id"), lookup_data),
        "current_rstatus": resolve_lookup('read_statuses', doc.get("current_rstatus_id"),
                                          lookup_data),
        "rstatus_history": make_subdocuments(doc.get("rstatus_history"), 'rstatus_history',
                                             subdoc_registry, separator=','),
        "date_started": doc.get("date_started"),
        "date_completed": doc.get("date_completed"),
        "rating": None if doc.get("rating") == "" else int(doc.get("rating")),
        "notes": doc.get("notes"),
    }
    return transformed_doc


def transform_user_roles_func(doc):
    """
    Transforms a user_roles document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "role_id": doc.get("role_id"),
        "role_name": doc.get("role_name"),
        "role_permissions": to_array(doc.get("role_permissions")),
        "role_description": doc.get("role_description"),
        "created_at": str(datetime.now())
    }

def transform_user_badges_func(doc):
    """
    Transforms a user_badges document to the desired structure.
    """
    return {
        "name": doc.get("badge_name"),
        "description": doc.get("badge_description"),
        "created_at": str(datetime.now())
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
    with open(TRANSFORMED_COLLECTIONS_DIR / "user_reads.json", "r", encoding="utf-8") as f:
        ur = json.load(f)
    with open(RAW_COLLECTIONS_DIR / "book_versions.json", "r", encoding="utf-8") as f:
        bv = json.load(f)
    ur = add_reading_rates(ur, bv)
    with open(TRANSFORMED_COLLECTIONS_DIR / "user_reads.json", "w", encoding="utf-8") as f:
        json.dump(ur, f, ensure_ascii=False, indent=2)
