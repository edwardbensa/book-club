# Import modules
import os
import csv
from loguru import logger
from pymongo.errors import ConfigurationError
from src.config import EXTRACTED_TABLES_DIR
from src.elt.utils import connect_mongodb


# Connect to MongoDB
db, client = connect_mongodb()

# Drop all existing collections
for name in db.list_collection_names():
    db.drop_collection(name)
    logger.info(f"Dropped collection '{name}'")


def load_csv_to_mongo():
    """
    Loads all CSV files in the specified directory into MongoDB.
    Each file is loaded into a collection named after the file (without .csv).
    """
    for filename in os.listdir(EXTRACTED_TABLES_DIR):
        if not filename.endswith(".csv"):
            continue

        collection_name = os.path.splitext(filename)[0]
        file_path = os.path.join(EXTRACTED_TABLES_DIR, filename)

        try:
            with open(file_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                records = [row for row in reader if any(row.values())]  # Skip empty rows

            if records:
                db[collection_name].insert_many(records)
                logger.info(f"Inserted {len(records)} records into '{collection_name}'")
            else:
                logger.warning(f"No records found in '{filename}'")

        except ConfigurationError as e:
            logger.error(f"Failed to load '{filename}': {e}")

if __name__ == "__main__":
    load_csv_to_mongo()
    logger.info("Loaded all extracted CSVs into MongoDB")
