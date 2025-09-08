# Import modules
import os
import csv
import time
import shutil
import gspread
from loguru import logger
from src.db.utils.connectors import connect_googlesheet
from src.config import EXTRACTED_TABLES_DIR

# Connect to Book CLub DB spreadsheet
spreadsheet = connect_googlesheet()


# Function to delete existing tables in the local directory
def delete_existing_tables():
    """
    Deletes all files from the specified directory
    to clean directory before new tables are downloaded.
    """
    logger.info(f"Checking for existing tables in '{EXTRACTED_TABLES_DIR}'...")
    try:
        # Get all entries in the directory
        files = os.listdir(EXTRACTED_TABLES_DIR)
        if files:
            logger.info(f"Found {len(files)} existing files. Deleting them now...")
            # Delete directory and all its contents
            shutil.rmtree(EXTRACTED_TABLES_DIR)
            # Recreate the directory after deletion
            os.makedirs(EXTRACTED_TABLES_DIR)
            logger.success("Successfully deleted all existing tables.")
        else:
            logger.info("Directory is empty. No deletion needed.")
    except OSError as e:
        logger.error(f"Error deleting files from '{EXTRACTED_TABLES_DIR}': {e}")
        # If deletion fails, stop the process to prevent errors later
        exit()


def extract_sheets(sheet_names):
    """
    Extracts data from specified sheets in Book Club DB and saves them to EXTRACTED_TABLES_DIR.
    """
    for name in sheet_names:
        try:
            sheet = spreadsheet.worksheet(name) # type: ignore
            records = sheet.get_all_records()

            # Save to CSV
            if records:
                csv_path = os.path.join(EXTRACTED_TABLES_DIR, f"{name}.csv")
                with open(csv_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
                logger.info(f"Saved {len(records)} records to '{csv_path}'")
            else:
                logger.warning(f"No records found in sheet '{name}'")

        except gspread.exceptions.APIError as e:
            logger.error(f"APIError for sheet '{name}': {e}")


# Import sheets
book_sheets = [
    "books", "creators", "creator_roles", "genres", "book_collections",
    "awards", "award_categories", "award_statuses", "publishers",
    "formats", "tags", "cover_art", "languages"
]

user_sheets = [
    "users", "user_reads", "read_statuses", "user_badges"
]

club_sheets = [
    "clubs", "club_members", "club_member_roles", "club_member_reads",
    "club_reads", "club_reading_periods", "club_period_books", "club_discussions",
    "club_events", "club_event_types", "club_event_statuses", "club_badges"
]


if __name__ == "__main__":
    delete_existing_tables()
    extract_sheets(book_sheets)
    time.sleep(15)
    extract_sheets(user_sheets)
    time.sleep(30)
    extract_sheets(club_sheets)
    logger.info("Saved tables to disk")
