# Import modules
import re
from src.db.utils.transforms import transform_collection
from src.db.utils.parsers import to_int, to_float, to_array, make_subdocuments
from src.db.utils.lookups import (load_lookup_data, resolve_lookup, resolve_creator,
                                  resolve_awards, generate_image_url)


# Load and map all lookup collections
lookup_registry = {
    'creators': {'field': 'creator_id', 'get': ['_id', 'creator_firstname', 'creator_lastname']},
    'book_collections': {'field': 'bcollection_name', 'get': ['_id', 'bcollection_name']},
    'awards': {'field': 'award_id', 'get': '_id'},
    'publishers': {'field': 'publisher_name', 'get': ['_id', 'publisher_name']},
    'books': {'field': 'book_id', 'get': '_id'},
}

lookup_data = load_lookup_data(lookup_registry)


# Subdoc registry
subdoc_registry = {
    'authors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'author', lookup_data)
    },
    'translators': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'translator', lookup_data)
    },
    'narrators': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'narrator', lookup_data)
    },
    'illustrators': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'illustrator', lookup_data)
    },
    'cover_artists': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'cover_artist', lookup_data)
    },
    'editors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'editor', lookup_data)
    },
    'contributors': {
        'pattern': None,
        'transform': lambda name: resolve_creator(name.strip(), 'contributor', lookup_data)
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
        "author": make_subdocuments(doc.get("author"), "authors", subdoc_registry, separator=','),
        "genre": to_array(doc.get("genre")),
        "collection": resolve_lookup('book_collections', doc.get("collection"), lookup_data),
        "collection_index": to_int(doc.get("collection_index")),
        "description": doc.get("description"),
        "first_publication_date": doc.get("first_publication_date"),
        "contributors": make_subdocuments(doc.get("contributors"), "contributors", subdoc_registry, separator=','),
        "awards": make_subdocuments(doc.get("awards"), "awards", subdoc_registry, separator='|'),
        "tags": to_array(doc.get("tags")),
    }

def transform_book_variants_func(doc):
    """
    Transforms a book_variants document to the desired structure.
    """
    return{
        "_id": doc.get("_id"),
        "variant_id": doc.get("variant_id"),
        "book_id": resolve_lookup('books', doc.get("book_id"), lookup_data),
        "title": doc.get("title"),
        "isbn_13": doc.get("isbn_13"),
        "asin": doc.get("asin"),
        "format": doc.get("format"),
        "edition": doc.get("edition"),
        "release_date": doc.get("release_date"),
        "page_count": to_int(doc.get("page_count")),
        "length_hours": to_float(doc.get("length_hours")),
        "description": doc.get("description"),
        "publisher": resolve_lookup('publishers', doc.get("publisher"), lookup_data),
        "language": doc.get("language"),
        "translator": make_subdocuments(doc.get("translator"), "translators",
                                        subdoc_registry, separator=','),
        "narrator": make_subdocuments(doc.get("narrator"), "narrators",
                                      subdoc_registry, separator=','),
        "illustrator": make_subdocuments(doc.get("illustrator"), "illustrators",
                                         subdoc_registry, separator=','),
        "editors": make_subdocuments(doc.get("editors"), "editors",
                                     subdoc_registry, separator=','),
        "cover_artist": make_subdocuments(doc.get("cover_artist"), "cover_artists",
                                          subdoc_registry, separator=','),
        "cover_url": generate_image_url(doc, doc.get("cover_url"), "cover", "cover-art"),
        "date_added": doc.get("date_added")
    }

if __name__ == "__main__":
    transform_collection("books", transform_books_func)
    transform_collection("book_variants", transform_book_variants_func)
