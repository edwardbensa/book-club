"""Transform creators"""

# Imports
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
        "firstname": doc.get("firstname"),
        "lastname": doc.get("lastname"),
        "bio": doc.get("bio"),
        "website": doc.get("website"),
        "roles": to_array(doc.get("roles"))
    }

# Run transformation
if __name__ == "__main__":
    transform_collection("creators", transform_creators_func)
