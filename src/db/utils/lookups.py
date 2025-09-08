import re
from typing import Any, Dict, Set, List
from loguru import logger
from src.db.utils.connectors import connect_mongodb

db, client = connect_mongodb()

def pre_fetch_and_cache(documents: List[Dict[str, Any]], lookup_registry):
    """
    Collects all unique IDs from the documents and pre-fetches the corresponding data
    from the database to create in-memory caches.
    """
    unique_ids: Dict[str, Set[str]] = {
        'creators': set(),
        'awards': set(),
        'award_categories': set(),
        'award_statuses': set(),
        'cover_art': set(),
    }
    
    # Non-custom-ID fields that still require a lookup
    unique_names: Dict[str, Set[str]] = {
        'book_collections': set(),
        'publishers': set(),
    }

    # Collect all unique IDs and names from the documents
    for doc in documents:
        # Collect IDs for fields that use a custom ID (creators, cover_art)
        if doc.get('author'):
            unique_ids['creators'].update([i.strip() for i in doc['author'].split(',')])
        if doc.get('contributors'):
            unique_ids['creators'].update([i.strip() for i in doc['contributors'].split(',')])
        if doc.get('cover_art'):
            unique_ids['cover_art'].add(doc['cover_art'].strip())
        
        # Handle awards field which has multiple IDs in each entry
        if doc.get('awards'):
            award_entries = doc['awards'].split('|')
            for entry in award_entries:
                match = re.search(r'(aw\d+),\s*(ac\d+),\s*(\d{4}),\s*(as\d+)', entry)
                if match:
                    unique_ids['awards'].add(match.group(1))
                    unique_ids['award_categories'].add(match.group(2))
                    unique_ids['award_statuses'].add(match.group(4))

        # Collect names for fields that use a name as a lookup field
        if doc.get('collection'):
            unique_names['book_collections'].add(doc['collection'].strip())
        if doc.get('publisher'):
            unique_names['publishers'].add(doc['publisher'].strip())

    # Fetch all data from the database using a single query per collection
    caches: Dict[str, Dict[str, Any]] = {}

    for collection_name, ids in unique_ids.items():
        if ids:
            caches[collection_name] = {
                d[f'{collection_name.rstrip("s")}_id'.replace('creators_id', 'creator_id')]: d
                for d in db[collection_name].find({f'{collection_name.rstrip("s")}_id'.replace('creators_id', 'creator_id'): {'$in': list(ids)}})
            }
            logger.info(f"Pre-fetched {len(caches[collection_name])} documents for '{collection_name}'.")

    for collection_name, names in unique_names.items():
        if names:
            field_name = lookup_registry[collection_name]['string_field']
            caches[collection_name] = {
                d[field_name]: d
                for d in db[collection_name].find({field_name: {'$in': list(names)}})
            }
            logger.info(f"Pre-fetched {len(caches[collection_name])} documents for '{collection_name}'.")

    return caches