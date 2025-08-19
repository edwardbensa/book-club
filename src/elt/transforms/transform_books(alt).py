# Import modules
import re
from datetime import datetime
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from src.elt.transforms.utils import connect_mongodb

# Connect to MongoDB
db, client = connect_mongodb()

# Define the ID field mappings for books collection
def get_id_mappings(collection_names):
    """
    Fetches custom IDs and MongoDB ObjectIds from specified collections
    and returns a dictionary of mappings.
    """
    mappings = {}
    for name in collection_names:
        collection = db[name]
        try:
            # Note: The mapping for 'creators' needs to be a dictionary of full names for denormalization
            if name == 'creators':
                mappings[name] = {doc['creator_id']: f"{doc['creator_firstname']} {doc['creator_lastname']}" for doc in collection.find({})}
                logger.info(f"Created name mapping for '{name}' collection.")
                continue

            id_field_map = {
                'genres': 'genre_id',
                'tags': 'tag_id',
                'awards': 'award_id',
                'award_categories': 'acategory_id',
                'award_statuses': 'astatus_id',
                'book_collections': 'bcollection_id',
                'formats': 'format_id',
                'publishers': 'publisher_id',
                'cover_art': 'cart_id',
                'languages': 'language_id'
            }
            id_field = id_field_map.get(name)

            cursor = collection.find({}, {id_field: 1, '_id': 1, f"{name.rstrip('s')}_name": 1})

            # Collections where actual name instead of ObjectId should be returned
            if name in ['formats', 'genres', 'languages', 'publishers']:
                mappings[name] = {doc[id_field]: doc[f"{name.rstrip('s')}_name"] for doc in cursor if id_field in doc}
            else:
                mappings[name] = {doc[id_field]: doc['_id'] for doc in cursor if id_field in doc}

            logger.info(f"Created mapping for '{name}' collection.")
        except (ConnectionFailure, ConfigurationError) as e:
            logger.warning(f"Could not create mapping for '{name}': {e}")
            mappings[name] = {}
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Unexpected error while creating mapping for '{name}': {e}")
            mappings[name] = {}
    return mappings

# Define the collections to get mappings for
collections_to_map = [
    "genres", "creators", "tags", "awards", "award_categories",
    "award_statuses", "book_collections", "formats", "publishers",
    "cover_art", "languages"
]

# Get the id mappings from the database
id_mappings = get_id_mappings(collections_to_map)

# Functions to transform data

def parse_multi_value_field(field_string, collection_name):
    """
    Parses a comma-separated string of IDs and converts them into a list of
    MongoDB ObjectIds or names using the actual mapping dictionaries from the database.
    """
    if not field_string:
        return []

    ids = [item.strip() for item in field_string.split(',')]
    mapped_values = [
        id_mappings[collection_name].get(old_id)
        for old_id in ids if old_id in id_mappings[collection_name]
    ]
    return mapped_values

def parse_awards(awards_string):
    """
    Parses the complex awards string into an array of embedded documents.
    """
    if not awards_string:
        return []

    awards_list = []
    # Use a regex to extract the parts of each award entry
    pattern = re.compile(r'(aw\d+),\s*(ac\d+),\s*(\d{4}),\s*(as\d+)')

    for match in pattern.finditer(awards_string):
        award_id, category_id, year, status_id = match.groups()

        # Create an embedded document with ObjectId references and the year
        if category_id == 'ac001':
            award_doc = {
            "award_id": id_mappings["awards"].get(award_id),
            "year": int(year),
            "status_id": id_mappings["award_statuses"].get(status_id)
        }
        else:
            award_doc = {
            "award_id": id_mappings["awards"].get(award_id),
            "category_id": id_mappings["award_categories"].get(category_id),
            "year": int(year),
            "status_id": id_mappings["award_statuses"].get(status_id)
        }

        # Only add the document if the IDs were successfully mapped
        if all(award_doc.values()):
            awards_list.append(award_doc)

    return awards_list

def parse_formats(formats_string):
    """
    Parses the new 'format' string into a list of embedded documents.
    """
    if not formats_string:
        return []

    formats_list = []
    raw_formats = [f.strip() for f in formats_string.split('|')]

    for raw_format in raw_formats:
        format_doc = {}
        # Split by comma to get key:value pairs
        parts = [p.strip() for p in raw_format.split(',')]

        for part in parts:
            if ':' in part:
                key, value = [v.strip() for v in part.split(':', 1)]

                # Convert keys and values
                if key == 'format':
                    format_doc['format_name'] = id_mappings['formats'].get(value)
                elif key == 'edition':
                    format_doc['edition'] = value
                elif key == 'page_count':
                    format_doc['page_count'] = int(value) if value.isdigit() else None
                elif key == 'length':
                    format_doc['length'] = value
                elif key == 'language':
                    format_doc['language_name'] = id_mappings['languages'].get(value)
                elif key == 'publisher':
                    format_doc['publisher_name'] = id_mappings['publishers'].get(value)
                elif key == 'cover_art':
                    format_doc['cover_art'] = id_mappings['cover_art'].get(value)
                elif key == 'isbn_13':
                    format_doc['isbn_13'] = value
                elif key == 'asin':
                    format_doc['asin'] = value
                elif key == 'release_date':
                    try:
                        format_doc['release_date'] = datetime.strptime(value, '%Y-%m-%d')
                    except ValueError:
                        format_doc['release_date'] = None
                elif key == 'translator':
                    format_doc['translator'] = parse_multi_value_field(value, "creators")
                elif key == 'narrator':
                    format_doc['narrator'] = parse_multi_value_field(value, "creators")
                elif key == 'illustrator':
                    format_doc['illustrator'] = parse_multi_value_field(value, "creators")
                elif key == 'cover artist':
                    format_doc['cover artist'] = parse_multi_value_field(value, "creators")
                elif key == 'editors':
                    format_doc['editors'] = parse_multi_value_field(value, "creators")

        # Only append the document if it has any data
        if format_doc:
            formats_list.append(format_doc)

    return formats_list

# Transform and import books

def transform_and_import_books():
    """
    Main function to fetch raw data from the 'books' collection, transform it,
    and insert it into a temporary collection before replacing the original.
    """
    try:
        raw_books_collection = db["books"]
        books_data = list(raw_books_collection.find({}))
        logger.info(f"Fetched {len(books_data)} records from 'books' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'books' collection: {e}")
        return

    transformed_books = []
    for book in books_data:
        try:
            # New structure without top-level format-specific fields
            transformed_doc = {
                "title": book.get("book_title"),
                "author": parse_multi_value_field(book.get("author"), "creators"),
                "genre_name": parse_multi_value_field(book.get("genre"), "genres"),
                "collection": id_mappings["book_collections"].get(book.get("collection")),
                "collection_index": book.get("collection_index"),
                "description": book.get("description"),
                "first_publication_date": datetime.strptime(book.get("first_publication_date"), '%Y-%m-%d') if book.get("first_publication_date") else None,
                "tags": parse_multi_value_field(book.get("tags"), "tags"),
                "awards": parse_awards(book.get("awards")),
                "contributors": parse_multi_value_field(book.get("contributors"), "creators"),
                "formats": parse_formats(book.get("format"))
            }

            # Remove keys with None, empty lists, or empty strings
            cleaned_doc = {k: v for k, v in transformed_doc.items()
                           if v is not None and v != [] and v != ''}
            transformed_books.append(cleaned_doc)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to transform book data for book_id {book.get('book_id')}: {e}")
            continue

    if transformed_books:
        # Drop the existing 'books' collection and insert transformed collection
        db.drop_collection("books")
        logger.info("Dropped existing 'books' collection.")

        db["books"].insert_many(transformed_books)
        logger.info(f"Successfully imported {len(transformed_books)} transformed books into the 'books' collection.")
    else:
        logger.warning("No books were transformed or imported.")

if __name__ == "__main__":
    transform_and_import_books()
