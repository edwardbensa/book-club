# Import modules
import re
from datetime import datetime
from src.db.utils.parsers import to_int, make_subdocuments, to_array
from src.db.utils.transforms import transform_collection
from src.db.utils.lookups import load_lookup_data

# Define lookup registry
lookup_registry = {
    "books": {"field": "book_id", "get": "_id"},
    "users": {"field": "user_id", "get": "_id"},
    "clubs": {"field": "club_id", "get": "_id"},
    "club_reading_periods": {"field": "period_id", "get": "_id"},
    "genres": {"field": "genre_name", "get": ["_id", "genre_name"]}
}

# Load lookup data
lookup_data = load_lookup_data(lookup_registry)

# Subdoc registry
subdoc_registry = {
    "votes": {
        "pattern": re.compile(r"user_id:\s*(\w+),\s*vote_date:\s*(\d{4}-\d{2}-\d{2})"),
        "transform": lambda match: {
            "user_id": lookup_data["users"].get(match.group(1)),
            "timestamp": match.group(2)
        }
    },
    "club_discussions": {
        "pattern": re.compile(
            r"user_id:\s*(\w+);\s*comment:\s*(.+?);\s*timestamp:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})"
        ),
        "transform": lambda match: {
            "user_id": lookup_data["users"].get(match.group(1)),
            "comment": match.group(2).strip(),
            "timestamp": match.group(3)
        }
    },
    "club_genres": {
        "pattern": None,
        "transform": lambda genre_name: lookup_data["genres"].get(genre_name.strip())
    }
}

# Transformation functions
def transform_club_members_func(doc):
    """
    Transforms a club_members document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "user_id": lookup_data["users"].get(doc.get("user_id")),
        "role": doc.get("role"),
        "date_joined": doc.get("date_joined"),
        "is_active": doc.get("is_active"),
        "created_at": str(datetime.now())
    }

def transform_club_member_roles_func(doc):
    """
    Transforms a club_member_roles document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "role_id": doc.get("role_id"),
        "role_name": doc.get("role_name"),
        "role_permissions": to_array(doc.get("role_permissions")),
        "role_description": doc.get("role_description"),
        "created_at": str(datetime.now())
    }

def transform_club_member_reads_func(doc):
    """
    Transforms a club_member_reads document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "user_id": lookup_data["users"].get(doc.get("user_id")),
        "book_id": lookup_data["books"].get(doc.get("book_id")),
        "period_id": lookup_data["club_reading_periods"].get(doc.get("period_id")),
        "read_date": doc.get("read_date"),
        "created_at": str(datetime.now())
    }

def transform_club_reads_func(doc):
    """
    Transforms a club_reads document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "book_id": lookup_data["books"].get(doc.get("book_id")),
        "period_id": lookup_data["club_reading_periods"].get(doc.get("period_id")),
        "period_enddate": doc.get("period_enddate"),
        "created_at": str(datetime.now())
    }

def transform_club_period_books_func(doc):
    """
    Transforms a club_period_books document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "book_id": lookup_data["books"].get(doc.get("book_id")),
        "period_id": lookup_data["club_reading_periods"].get(doc.get("period_id")),
        "selected_by": lookup_data["users"].get(doc.get("user_id")),
        "selection_method": doc.get("selection_method"),
        "votes": make_subdocuments(doc.get("votes"), "votes", subdoc_registry, separator=";"),
        "votes_startdate": doc.get("votes_startdate"),
        "votes_enddate": doc.get("votes_enddate"),
        "selection_status": doc.get("selection_status"),
        "created_at": str(datetime.now())
    }

def transform_club_discussions_func(doc):
    """
    Transforms a club_discussions document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "topic_name": doc.get("topic_name"),
        "topic_description": doc.get("topic_description"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "created_at": doc.get("created_at"),
        "comments": make_subdocuments(doc.get("comments"), "club_discussions", subdoc_registry, separator="|"),
        "book_reference": lookup_data["books"].get(doc.get("book_reference"))
    }

def transform_club_events_func(doc):
    """
    Transforms a club_events document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "event_name": doc.get("event_name"),
        "event_description": doc.get("event_description"),
        "event_type": doc.get("event_type"),
        "event_startdate": doc.get("event_startdate"),
        "event_enddate": doc.get("event_enddate"),
        "event_status": doc.get("event_status"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "created_at": str(datetime.now())
    }

def transform_club_reading_periods_func(doc):
    """
    Transforms a club_reading_periods document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "period_name": doc.get("period_name"),
        "period_description": doc.get("period_description"),
        "period_startdate": doc.get("period_startdate"),
        "period_enddate": doc.get("period_enddate"),
        "period_status": doc.get("period_status"),
        "max_books": to_int(doc.get("max_books")),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "created_at": str(datetime.now())
    }

def transform_clubs_func(doc):
    """
    Transforms a clubs document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_handle": doc.get("club_handle"),
        "club_name": doc.get("club_name"),
        "club_creationdate": doc.get("club_creationdate"),
        "club_genres": make_subdocuments(doc.get("club_genres"), "club_genres",
                                         subdoc_registry, separator=","),
        "club_description": doc.get("club_description"),
        "club_visibility": doc.get("club_visibility"),
        "club_rules": doc.get("club_rules"),
        "club_moderators": [lookup_data["users"].get(user) for user
                            in to_array(doc.get("club_moderators"))],
        "club_badges": doc.get("club_badges"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "created_at": str(datetime.now())
    }

# Run all transformations
if __name__ == "__main__":
    transform_collection("club_members", transform_club_members_func)
    transform_collection("club_member_roles", transform_club_member_roles_func)
    transform_collection("club_member_reads", transform_club_member_reads_func)
    transform_collection("club_reads", transform_club_reads_func)
    transform_collection("club_period_books", transform_club_period_books_func)
    transform_collection("club_discussions", transform_club_discussions_func)
    transform_collection("club_events", transform_club_events_func)
    transform_collection("club_reading_periods", transform_club_reading_periods_func)
    transform_collection("clubs", transform_clubs_func)
