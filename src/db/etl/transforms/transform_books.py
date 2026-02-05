"""Transform books"""

# Imports
import re
import json
from src.db.utils.transforms import transform_collection
from src.db.utils.parsers import to_int, to_float, to_array, make_subdocuments
from src.db.utils.lookups import (load_lookup_data, resolve_lookup, resolve_creator,
                                  resolve_awards)
from src.db.utils.derived_fields import generate_image_url
from src.db.utils.connectors import connect_azure_blob
from src.config import RAW_COLLECTIONS_DIR

# Blob Service Client
blobserviceclient_account_name = connect_azure_blob().account_name

# Load and map all lookup collections
lookup_registry = {
    'creators': {'field': 'creator_id', 'get': ['_id', 'firstname', 'lastname']},
    'book_series': {'field': 'name', 'get': ['_id', 'name']},
    'awards': {'field': 'award_id', 'get': '_id'},
    'publishers': {'field': 'name', 'get': ['_id', 'name']},
    'books': {'field': 'book_id', 'get': '_id'},
}

lookup_data = load_lookup_data(lookup_registry)


# Subdoc registry
subdoc_registry = {
    'creators': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), lookup_data)
    },
    'awards': {
        'pattern': re.compile(
            r"award_id:\s*(\w+);\s*"
            r"award_name:\s*(.*?);\s*"
            r"award_category:\s*(.*?);\s*"
            r"year:\s*(\d{4});\s*"
            r"award_status:\s*(\w+)"
        ),
        'transform': lambda match: resolve_awards(match, lookup_data)
    }
}

# Transform function
def transform_books_func(doc):
    """
    Transforms a books document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "book_id": doc.get("book_id"),
        "title": doc.get("title"),
        "author": make_subdocuments(doc.get("author"), "creators", subdoc_registry, separator=','),
        "genre": to_array(doc.get("genre")),
        "series": resolve_lookup('book_series', doc.get("series"), lookup_data),
        "series_index": to_int(doc.get("series_index")),
        "description": doc.get("description"),
        "first_publication_date": doc.get("first_publication_date"),
        "contributors": make_subdocuments(doc.get("contributors"), "creators", subdoc_registry,','),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_registry, separator='|'),
        "tags": to_array(doc.get("tags")),
        "date_added": doc.get("date_added")
    }

def transform_book_versions_func(doc):
    """
    Transforms a book_versions document to the desired structure.
    """
    return{
        "_id": doc.get("_id"),
        "version_id": doc.get("version_id"),
        "book_id": resolve_lookup('books', doc.get("book_id"), lookup_data),
        "title": doc.get("title"),
        "isbn_13": to_int(doc.get("isbn_13")),
        "asin": doc.get("asin"),
        "format": doc.get("format"),
        "edition": doc.get("edition"),
        "release_date": doc.get("release_date"),
        "page_count": to_int(doc.get("page_count")),
        "length_hours": to_float(doc.get("length")),
        "description": doc.get("description"),
        "publisher": resolve_lookup('publishers', doc.get("publisher"), lookup_data),
        "language": doc.get("language"),
        "translator": make_subdocuments(doc.get("translator"), "creators", subdoc_registry, ','),
        "narrator": make_subdocuments(doc.get("narrator"), "creators", subdoc_registry, ','),
        "illustrator": make_subdocuments(doc.get("illustrator"), "creators", subdoc_registry, ','),
        "editors": make_subdocuments(doc.get("editors"), "creators", subdoc_registry, ','),
        "cover_artist": make_subdocuments(doc.get("cover_artist"), "creators", subdoc_registry,','),
        "cover_url": generate_image_url(doc, doc.get("cover_url"), "cover", "cover-art",
                                        blobserviceclient_account_name),
        "date_added": doc.get("date_added")
    }

with open(RAW_COLLECTIONS_DIR / "books.json", "r", encoding="utf-8") as f:
    books = json.load(f)

def transform_book_series_func(doc):
    """
    Transforms a book_series document to the desired structure.
    """
    filtered_books = [b for b in books if b["series"] == doc.get("name")]
    filtered_books.sort(key=lambda b: b["series_index"])
    selected = [{"index": to_int(b["series_index"]), "_id": b["_id"]} for b in filtered_books]

    return{
        "_id": doc.get("_id"),
        "name": doc.get("name"),
        "books": selected,
        "date_added": doc.get("date_added")
    }

if __name__ == "__main__":
    transform_collection("books", transform_books_func)
    transform_collection("book_versions", transform_book_versions_func)
    transform_collection("book_series", transform_book_series_func)
