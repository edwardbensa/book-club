# Import modules
from src.db.utils.parsers import to_array
from src.db.utils.transforms import transform_collection


# Transform function
def transform_creators_func(doc):
    """
    Transforms a creators document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "creator_id": doc.get("creator_id"),
        "creator_firstname": doc.get("creator_firstname"),
        "creator_lastname": doc.get("creator_lastname"),
        "creator_bio": doc.get("creator_bio"),
        "creator_website": doc.get("creator_website"),
        "creator_roles": to_array(doc.get("creator_roles"))
    }

# Run transformation
if __name__ == "__main__":
    transform_collection("creators", transform_creators_func)
