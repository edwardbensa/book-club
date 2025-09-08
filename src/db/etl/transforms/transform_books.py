# Import modules
import re
from src.db.utils.transforms import transform_collection
from src.db.utils.parsers import to_int, to_array, make_subdocuments
from src.db.utils.lookups import (load_lookup_data, resolve_lookup, resolve_creator,
                                  resolve_awards, resolve_format_entry)


# Load and map all lookup collections
lookup_registry = {
    'creators': {'field': 'creator_id', 'get': ['_id', 'creator_firstname', 'creator_lastname']},
    'book_collections': {'field': 'bcollection_name', 'get': ['_id', 'bcollection_name']},
    'awards': {'field': 'award_id', 'get': ['_id', 'award_name']},
    'award_categories': {'field': 'acategory_id', 'get': 'acategory_name'},
    'award_statuses': {'field': 'astatus_id', 'get': 'astatus_name'},
    'cover_art': {'field': 'cart_id', 'get': '_id'},
    'publishers': {'field': 'publisher_name', 'get': ['_id', 'publisher_name']},
}

lookup_data = load_lookup_data(lookup_registry)


# Subdoc registry
subdoc_registry = {
    'authors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'author', lookup_data)
    },
    'awards': {
        'pattern': re.compile(r'(aw\d+),\s*(ac\d+),\s*(\d{4}),\s*(as\d+)'),
        'transform': lambda entry: resolve_awards(entry, lookup_data)
    },
    'contributors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'contributor', lookup_data)
    },
    'format': {
        'pattern': None,
        'transform': lambda entry: resolve_format_entry(entry, lookup_data)
    },
}

# Transform function
def transform_books_func(doc):
    """
    Transforms a books document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "book_id": doc.get("book_id"),
        "book_title": doc.get("book_title"),
        "author": make_subdocuments(doc.get("author"), "authors", subdoc_registry, separator=','),
        "genre": to_array(doc.get("genre")),
        "collection": resolve_lookup('book_collections', doc.get("collection"), lookup_data),
        "collection_index": to_int(doc.get("collection_index")),
        "description": doc.get("description"),
        "first_publication_date": doc.get("first_publication_date"),
        "contributors": make_subdocuments(doc.get("contributors"), "contributors", subdoc_registry, separator=','),
        "format": make_subdocuments(doc.get("format"), "format", subdoc_registry, separator='|'),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_registry, separator='|'),
        "tags": to_array(doc.get("tags")),
    }

if __name__ == "__main__":
    transform_collection("books", transform_books_func)
