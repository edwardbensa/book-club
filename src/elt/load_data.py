# Import modules
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from loguru import logger
from src.elt.utils import connect_mongodb

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))
gsheet_cred = os.getenv("GSHEET_CRED")


# Google Sheets authorisation
cred_file = gsheet_cred
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope) # type: ignore
client_sheet = gspread.authorize(creds)

# Open spreadsheet
spreadsheet = client_sheet.open("Book Club DB")
logger.info("Connected to Google Sheet")

# Connect to MongoDB
db, client = connect_mongodb()

# Drop all collections to refresh the database
existing_collections = db.list_collection_names()

for collection_name in existing_collections:
    db.drop_collection(collection_name)
    print(f"Dropped collection '{collection_name}'")

logger.info("Dropped all existing collections")

# Import sheets
sheets = [
    "books", "creators", "creator_roles", "genres", "book_collections",
    "awards", "award_categories", "award_statuses", "publishers",
    "formats", "tags", "cover_art", "languages", "users", "user_reads",
    "read_statuses"
]

for name in sheets:
    sheet = spreadsheet.worksheet(name)
    data = sheet.get_all_records()
    db[name].insert_many(data)
    print(f"Imported {len(data)} records into '{name}' collection.")
logger.info("Created collections in database")
