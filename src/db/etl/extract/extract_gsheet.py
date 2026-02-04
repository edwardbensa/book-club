"""Google Sheets data extraction"""

# Import modules
import os
import json
import time
import gspread
from bson.objectid import ObjectId
from loguru import logger
from src.db.utils.connectors import connect_googlesheet
from src.db.utils.files import wipe_directory
from src.config import RAW_COLLECTIONS_DIR

# Connect to Book Club DB spreadsheet
spreadsheet = connect_googlesheet()

# Function to extract sheets and save directly to JSON
def extract_sheets_to_json(sheet_names):
    """
    Extracts data from specified sheets, adds ObjectId, saves to RAW_COLLECTIONS_DIR as JSON.
    """
    for name in sheet_names:
        try:
            sheet = spreadsheet.worksheet(name)  # type: ignore
            records = sheet.get_all_records()

            if records:
                documents = []
                for row in records:
                    doc = {"_id": str(ObjectId())}
                    doc.update(row) # type: ignore
                    documents.append(doc)

                output_path = os.path.join(RAW_COLLECTIONS_DIR, f"{name}.json")
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(documents, f, ensure_ascii=False, indent=2)

                logger.info(f"Saved {len(documents)} records to '{output_path}'")
            else:
                logger.warning(f"No records found in sheet '{name}'")

        except gspread.exceptions.APIError as e:
            logger.error(f"APIError for sheet '{name}': {e}")

# Sheet groups
book_sheets = [
    "books", "book_versions", "creators", "creator_roles", "genres", "book_series",
    "awards", "publishers", "formats", "tags", "languages"
]

user_sheets = [
    "users", "user_reads", "read_statuses", "user_badges", "user_roles", "user_permissions"
]

club_sheets = [
    "clubs", "club_members", "club_member_reads", "club_reading_periods", "club_period_books",
    "club_discussions", "club_events", "club_event_types", "club_event_statuses", "club_badges"
]

other = ["countries"]

# Run extraction
if __name__ == "__main__":
    wipe_directory(RAW_COLLECTIONS_DIR)
    extract_sheets_to_json(book_sheets)
    time.sleep(10)
    extract_sheets_to_json(user_sheets)
    time.sleep(10)
    extract_sheets_to_json(club_sheets)
    extract_sheets_to_json(other)
    logger.success("All raw collections saved to disk.")
