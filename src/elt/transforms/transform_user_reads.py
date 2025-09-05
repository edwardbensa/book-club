# Import modules
import re
from src.elt.utils import (connect_mongodb, get_id_mappings, get_name_mappings,
                           to_datetime, transform_collection, make_subdocuments)

# Connect to MongoDB
db, client = connect_mongodb()

# Define the ID field mappings
id_field_map = {
    'books': 'book',
    'users': 'user_id'
}

# Define the name field mappings
name_field_map = {
    'read_statuses': 'rstatus_id'
}

field_name_lookup = {
                'read_statuses': 'rstatus_name'
            }

# Get the id and name mappings from the database
id_mappings = get_id_mappings(db, id_field_map, list(id_field_map.keys()))
name_mappings= get_name_mappings(db, name_field_map, field_name_lookup, list(name_field_map.keys()))


subdoc_registry = {
    'rstatus_history': {
        'pattern': re.compile(r'(.+):\s*(\d{4}-\d{2}-\d{2})'),
        'transform': lambda match: {
            "rstatus": name_mappings['read_statuses'].get(match.group(1)),
            "timestamp": to_datetime(match.group(2))
        }
    },
}


def transform_func(doc):
    """
    Transforms a user_read document to the desired structure.
    """
    transformed_doc = {
        "_id": doc.get("_id"),
        "user_id": id_mappings["users"].get(doc.get("user_id")),
        "book_id": id_mappings["books"].get(doc.get("book_id")),
        "current_rstatus": name_mappings["read_statuses"].get(doc.get("current_rstatus_id")),
        "rstatus_history": make_subdocuments(doc.get("rstatus_history"), 'rstatus_history',
                                             subdoc_registry, separator=','),
        "date_started": to_datetime(doc.get("date_started")),
        "date_completed": to_datetime(doc.get("date_completed")),
        "book_rating": None if doc.get("book_rating") == "" else int(doc.get("book_rating")),
        "notes": doc.get("notes"),
    }
    return transformed_doc


# Transform 'user_reads' collection
if __name__ == "__main__":
    transform_collection(db, "user_reads", transform_func)
