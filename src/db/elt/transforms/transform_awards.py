# Import modules
from loguru import logger
from src.db.utils.connectors import connect_mongodb
from src.db.utils.doc_transformers import get_id_mappings, parse_multi_value_field


# Connect to MongoDB
db, client = connect_mongodb()

# Define the ID field mappings for awards collection
id_field_map = {
                'award_categories': 'acategory_id',
                'award_statuses': 'astatus_id'
            }

# Define the collections to get mappings for
collections_to_map = list(id_field_map.keys())

# Get the id mappings from the database
id_mappings = get_id_mappings(db, id_field_map, collections_to_map)


# Transform 'awards' collection
def transform_awards():
    """
    Main function to fetch raw data from the 'awards' collection, transform it,
    and insert it into a temporary collection before replacing the original.
    """
    try:
        raw_awards_collection = db["awards"]
        awards_data = list(raw_awards_collection.find({}))
        logger.info(f"Fetched {len(awards_data)} records from 'awards' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'awards' collection: {e}")
        return

    transformed_awards = []
    for award in awards_data:
        try:
            # Create a new document with the desired structure
            transformed_doc = {
                "_id": award.get("_id"),
                "award_name": award.get("award_name"),
                "award_org": award.get("award_org"),
                "award_description": award.get("award_description"),
                "award_website": award.get("award_website"),
                "award_categories": None if award.get("award_categories") == "ac001" else parse_multi_value_field(
                    id_mappings, award.get("award_categories"), "award_categories"),
                "award_statuses": parse_multi_value_field(id_mappings, award.get("award_statuses"), "award_statuses"),
                "year_started": award.get("year_started"),
                "year_ended": award.get("year_ended")
            }

            # Remove keys with None, empty lists, or empty strings
            cleaned_doc = {k: v for k, v in transformed_doc.items() if v is not None and v != [] and v != ''}
            transformed_awards.append(cleaned_doc)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to transform award data for award_id {award.get('award_id')}: {e}")
            continue

    if transformed_awards:
        # Drop the existing 'awards' collection and insert transformed collection
        db.drop_collection("awards")
        logger.info("Dropped existing 'awards' collection.")

        db["awards"].insert_many(transformed_awards)
        logger.info(f"Successfully imported {len(transformed_awards)} transformed awards into the 'awards' collection.")
    else:
        logger.warning("No awards were transformed or imported.")

if __name__ == "__main__":
    transform_awards()
