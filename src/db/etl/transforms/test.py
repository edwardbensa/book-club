import re
from loguru import logger
from src.db.utils.parsers import to_datetime, to_int
from src.db.utils.lookups import build_lookup_map, transform_collection, resolve_lookup



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

lookup_data = {
    collection: build_lookup_map(collection, config['field'], config['get'])
    for collection, config in lookup_registry.items()
}

print(lookup_data)