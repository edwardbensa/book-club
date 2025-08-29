# Import modules
import re
from datetime import datetime
from loguru import logger
from src.elt.utils import connect_mongodb, get_id_mappings


# Connect to MongoDB
db, client = connect_mongodb()

# Define the ID field mappings for user_reads collection
id_field_map = {
                'books': 'book',
                'users': 'user_id',
                'read_statuses': 'rstatus_id',
            }

# Define the collections to get mappings for
collections_to_map = list(id_field_map.keys())

# Get the id mappings from the database
id_mappings = get_id_mappings(db, id_field_map, collections_to_map)

def parse_rstatus_history(history_string):
    """
    Parses a string of reading status history into a list of embedded documents.
    """
    if not history_string:
        return []

    history_list = []
    # Split the string by comma to get individual entries
    entries = history_string.split(';')
    for entry in entries:
        # Use regex to find the status ID and date
        match = re.match(r'(.+):\s*(\d{4}-\d{2}-\d{2})', entry.strip())
        if match:
            status_id, date_str = match.groups()
            history_list.append({
                "rstatus_id": id_mappings['read_statuses'].get(status_id),
                "timestamp": datetime.strptime(date_str, '%Y-%m-%d')
            })
    return history_list


# Transform 'user_reads' collection
def transform_user_reads():
    """
    Main function to fetch raw data from the 'user_reads' collection, transform it,
    and insert it into a temporary collection before replacing the original.
    """
    try:
        raw_user_reads_collection = db["user_reads"]
        user_reads_data = list(raw_user_reads_collection.find({}))
        logger.info(f"Fetched {len(user_reads_data)} records from 'user_reads' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'user_reads' collection: {e}")
        return

    transformed_user_reads = []
    for user_read in user_reads_data:
        try:
            # Create a new document with the desired structure
            transformed_doc = {
                "_id": user_read.get("_id"),
                "user_id": id_mappings["users"].get(user_read.get("user_id")),
                "book_id": id_mappings["books"].get(user_read.get("book_id")),
                "current_rstatus_id": id_mappings["read_statuses"].get(user_read.get("current_rstatus_id")),
                "rstatus_history": parse_rstatus_history(user_read.get("rstatus_history")),
                "date_started": datetime.strptime(user_read.get("date_started"), '%Y-%m-%d') if user_read.get("date_started") else None,
                "date_completed": datetime.strptime(user_read.get("date_completed"), '%Y-%m-%d') if user_read.get("date_completed") else None,
                "book_rating": None if user_read.get("book_rating") == "" else user_read.get("book_rating"),
                "notes": user_read.get("notes"),
            }

            transformed_user_reads.append(transformed_doc)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to transform user_read data for user_read_id {user_read.get('user_read_id')}: {e}")
            continue

    if transformed_user_reads:
        # Drop the existing 'user_reads' collection and insert transformed collection
        db.drop_collection("user_reads")
        logger.info("Dropped existing 'user_reads' collection.")

        db["user_reads"].insert_many(transformed_user_reads)
        logger.info(f"Successfully imported {len(transformed_user_reads)} transformed user_reads into the 'user_reads' collection.")
    else:
        logger.warning("No user_reads were transformed or imported.")

if __name__ == "__main__":
    transform_user_reads()
