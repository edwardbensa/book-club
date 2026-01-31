"""Transform clubs"""

# Imports
import re
from datetime import datetime
from src.db.utils.parsers import to_int, to_array, make_subdocuments
from src.db.utils.transforms import transform_collection
from src.db.utils.lookups import load_lookup_data, resolve_lookup

# Define lookup registry
lookup_registry = {
    "books": {"field": "book_id", "get": "_id"},
    "users": {"field": "user_id", "get": "_id"},
    "clubs": {"field": "club_id", "get": "_id"},
    "club_reading_periods": {"field": "period_id", "get": "_id"},
    "genres": {"field": "genre_name", "get": ["_id", "name"]},
    'club_badges': {'field': 'name', 'get': ['_id', 'name']},
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
    },
    "join_requests": {
        "pattern": re.compile(r"user_id:\s*(\w+),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})"),
        "transform": lambda match: {
            "user_id": lookup_data["users"].get(match.group(1)),
            "timestamp": match.group(2)
        }
    },
    'badges': {
        'pattern': re.compile(r'badge:\s*(.+?),\s*timestamp:\s*(\d{4}-\d{2}-\d{2})'),
        'transform': lambda match: {
            **resolve_lookup('club_badges', match.group(1), lookup_data), # type: ignore
            "timestamp": match.group(2)
        }
    },
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
        "is_active": doc.get("is_active") == "TRUE",
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
        "timestamp": str(datetime.now())
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
        "period_startdate": doc.get("period_startdate"),
        "period_enddate": doc.get("period_enddate"),
        "selected_by": lookup_data["users"].get(doc.get("user_id")),
        "selection_method": doc.get("selection_method"),
        "votes": make_subdocuments(doc.get("votes"), "votes", subdoc_registry, separator=";"),
        "votes_startdate": doc.get("votes_startdate"),
        "votes_enddate": doc.get("votes_enddate"),
        "selection_status": doc.get("selection_status"),
        "date_added": str(datetime.now())
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
        "timestamp": doc.get("timestamp"),
        "comments": make_subdocuments(doc.get("comments"), "club_discussions",
                                      subdoc_registry, separator="|"),
        "book_reference": lookup_data["books"].get(doc.get("book_reference"))
    }

def transform_club_events_func(doc):
    """
    Transforms a club_events document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "type": doc.get("type"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("status"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now())
    }

def transform_club_reading_periods_func(doc):
    """
    Transforms a club_reading_periods document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "club_id": lookup_data["clubs"].get(doc.get("club_id")),
        "name": doc.get("name"),
        "description": doc.get("description"),
        "startdate": doc.get("startdate"),
        "enddate": doc.get("enddate"),
        "status": doc.get("status"),
        "max_books": to_int(doc.get("max_books")),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
        "date_added": str(datetime.now())
    }

def transform_club_badges_func(doc):
    """
    Transforms a user_badges document to the desired structure.
    """
    return {
        "name": doc.get("name"),
        "description": doc.get("description"),
        "date_added": str(datetime.now())
    }

def transform_clubs_func(doc):
    """
    Transforms a clubs document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "handle": doc.get("handle"),
        "name": doc.get("name"),
        "creationdate": doc.get("creationdate"),
        "preferred_genres": to_array(doc.get("preferred_genres")),
        "description": doc.get("description"),
        "visibility": doc.get("visibility"),
        "rules": doc.get("rules"),
        "moderators": [lookup_data["users"].get(user) for user
                            in to_array(doc.get("moderators"))],
        "badges": make_subdocuments(doc.get("badges"), 'badges', subdoc_registry, separator='|'),
        "member_permissions": to_array(doc.get("member_permissions")),
        "join_requests": make_subdocuments(doc.get("join_requests"), "join_requests",
                                           subdoc_registry, separator=";"),
        "created_by": lookup_data["users"].get(doc.get("created_by")),
    }

# Run all transformations
if __name__ == "__main__":
    transform_collection("club_members", transform_club_members_func)
    transform_collection("club_member_reads", transform_club_member_reads_func)
    transform_collection("club_period_books", transform_club_period_books_func)
    transform_collection("club_discussions", transform_club_discussions_func)
    transform_collection("club_events", transform_club_events_func)
    transform_collection("club_reading_periods", transform_club_reading_periods_func)
    transform_collection("club_badges", transform_club_badges_func)
    transform_collection("clubs", transform_clubs_func)
