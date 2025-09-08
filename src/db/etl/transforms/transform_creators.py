# Import modules
from src.db.utils.parsers import make_array
from src.db.utils.transforms import transform_collection
from src.db.utils.lookups import load_lookup_data, resolve_lookup

# Define lookup registry
lookup_registry = {
    "creator_roles": {"field": "cr_id", "get": "cr_name"}
}

# Load lookup data
lookup_data = load_lookup_data(lookup_registry)

# Subdoc registry for array fields
array_registry = {
    "creator_roles": {
        "pattern": None,
        "transform": lambda val: resolve_lookup("creator_roles", val, lookup_data)
    }
}

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
        "creator_roles": make_array(doc.get("creator_roles"), "creator_roles", array_registry, ",")
    }

# Run transformation
if __name__ == "__main__":
    transform_collection("creators", transform_creators_func)
