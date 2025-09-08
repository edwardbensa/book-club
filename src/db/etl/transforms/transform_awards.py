# Import modules
from src.db.utils.parsers import make_array, to_int
from src.db.utils.transforms import transform_collection
from src.db.utils.lookups import load_lookup_data, resolve_lookup

# Define lookup registry
lookup_registry = {
    "award_categories": {"field": "acategory_id", "get": "acategory_name"},
    "award_statuses": {"field": "astatus_id", "get": "astatus_name"}
}

# Load lookup data
lookup_data = load_lookup_data(lookup_registry)

# Array registry for array fields
array_registry = {
    "award_categories": {
        "pattern": None,
        "transform": lambda val: resolve_lookup("award_categories", val, lookup_data)
    },
    "award_statuses": {
        "pattern": None,
        "transform": lambda val: resolve_lookup("award_statuses", val, lookup_data)
    }
}

# Transform function
def transform_awards_func(doc):
    """
    Transforms an awards document to the desired structure.
    """
    return {
        "_id": doc.get("_id"),
        "award_name": doc.get("award_name"),
        "award_org": doc.get("award_org"),
        "award_description": doc.get("award_description"),
        "award_website": doc.get("award_website"),
        "award_categories": None if doc.get("award_categories") == "ac001"
            else make_array(doc.get("award_categories"), "award_categories",
                            array_registry, separator=','),
        "award_statuses": make_array(doc.get("award_statuses"), "award_statuses",
                                     array_registry, separator=','),
        "year_started": to_int(doc.get("year_started")),
        "year_ended": to_int(doc.get("year_ended"))
    }

# Run transformation
if __name__ == "__main__":
    transform_collection("awards", transform_awards_func)
