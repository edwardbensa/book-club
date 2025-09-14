# Import modules
from src.db.utils.parsers import to_int, to_array
from src.db.utils.transforms import transform_collection


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
        "award_categories": to_array(doc.get("award_categories")),
        "award_statuses": to_array(doc.get("award_statuses")),
        "year_started": to_int(doc.get("year_started")),
        "year_ended": to_int(doc.get("year_ended"))
    }

# Run transformation
if __name__ == "__main__":
    transform_collection("awards", transform_awards_func)
