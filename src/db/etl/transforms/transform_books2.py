# Import modules
import re
from src.db.utils.parsers import to_datetime, to_int
from src.db.utils.doc_transformers import (resolve_lookup, resolve_creator, refresh_collection, make_array_field,
                                           resolve_format_entry, resolve_awards,
                                           make_subdocuments, transform_collection)


# Define field lookups
lookup_registry = {
    'creators': {
        'string_field': 'creator_id',
        'get': ['_id', 'creator_firstname', 'creator_lastname']
        },
    'book_collections': {
        'string_field': 'bcollection_name',
        'get': ['_id', 'bcollection_name']
        },
    'awards': {
        'string_field': 'award_id',
        'get': ['_id', 'award_name']
        },
    'award_categories': {
        'string_field': 'acategory_id',
        'get': 'acategory_name'
        },
    'award_statuses': {
        'string_field': 'astatus_id',
        'get': 'astatus_name'
        },
    'cover_art': {
        'string_field': 'cart_id',
        'get': '_id'
        },
    'publishers': {
        'string_field': 'publisher_name',
        'get': ['_id', 'publisher_name']
        },
}


subdoc_registry = {
    'authors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'author')
    },
    'awards': {
        'pattern': re.compile(r'(aw\d+),\s*(ac\d+),\s*(\d{4}),\s*(as\d+)'),
        'transform': lambda entry: resolve_awards(entry, lookup_registry)
    },
    'contributors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'contributor')
    },
    'format': {
        'pattern': None,
        'transform': lambda entry: resolve_format_entry(entry, lookup_registry)
    },
}


def transform_books_func(doc):
    """
    Transforms a books document to the desired structure.
    """
    transformed_doc = {
        "_id": doc.get("_id"),
        "book_id": doc.get("book_id"),
        "book_title": doc.get("book_title"),
        "author": make_subdocuments(doc.get("author"), "authors", subdoc_registry, separator=','),
        "genre": make_array_field(doc.get("genre")),
        "collection": None if doc.get("collection") == '' else resolve_lookup(
            'book_collections', doc.get("collection"), lookup_registry),
        "collection_index": to_int(doc.get("collection_index")),
        "description": doc.get("description"),
        "first_publication_date": to_datetime(doc.get("first_publication_date")),
        "contributors": make_subdocuments(doc.get("contributors"), "contributors",
                                          subdoc_registry, separator=','),
        "format": make_subdocuments(doc.get("format"), "format", subdoc_registry, separator='|'),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_registry, separator='|'),
        "tags": make_array_field(doc.get("tags")),
    }
    return transformed_doc

# Transform 'books' collection
if __name__ == "__main__":
    refresh_collection('books')
    transform_collection("books", transform_books_func)
