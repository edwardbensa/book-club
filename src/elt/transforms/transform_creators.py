# Import modules
from loguru import logger
from src.elt.transforms.utils import connect_mongodb, get_id_mappings


# Connect to MongoDB
db, client = connect_mongodb()

# Define the ID field mappings for creators collection
id_field_map = {
                'creator_roles': 'cr_id',
            }

# Define the collections to get mappings for
collections_to_map = list(id_field_map.keys())

# Get the id mappings from the database
id_mappings = get_id_mappings(db, id_field_map, collections_to_map)


# Transform 'creators' collection
def transform_creators():
    """
    Main function to fetch raw data from the 'creators' collection, transform it,
    and insert it into a temporary collection before replacing the original.
    """
    try:
        raw_creators_collection = db["creators"]
        creators_data = list(raw_creators_collection.find({}))
        logger.info(f"Fetched {len(creators_data)} records from 'creators' collection.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'creators' collection: {e}")
        return

    transformed_creators = []
    for creator in creators_data:
        try:
            # Create a new document with the desired structure
            transformed_doc = {
                "_id": creator.get("_id"),
                "creator_firstname": creator.get("creator_firstname"),
                "creator_lastname": creator.get("creator_lastname"),
                "creator_bio": creator.get("creator_bio"),
                "creator_website": creator.get("creator_website"),
                "creator_roles": id_mappings["creator_roles"].get(creator.get("creator_roles")),
            }

            transformed_creators.append(transformed_doc)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Failed to transform data for creator_id {creator.get('creator_id')}: {e}")
            continue

    if transformed_creators:
        # Drop the existing 'creators' collection and insert transformed collection
        db.drop_collection("creators")
        logger.info("Dropped existing 'creators' collection.")

        db["creators"].insert_many(transformed_creators)
        logger.info(f"Successfully imported {len(transformed_creators)} transformed creators into the 'creators' collection.")
    else:
        logger.warning("No creators were transformed or imported.")

if __name__ == "__main__":
    transform_creators()
