# Import modules
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

gsheet_cred = os.getenv("GSHEET_CRED")
mongodb_uri = os.getenv("MONGODB_URI")
azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


def connect_mongodb():
    """
    Connects to the MongoDB database and returns the database object.
    """
    try:
        client = MongoClient(mongodb_uri)
        db = client["book_club"]
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return db, client
    except (ConnectionFailure, ConfigurationError)  as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        exit()


def connect_azure_blob():
    """
    Connects to Azure Blob Storage and returns the BlobServiceClient.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_storage_connection_string) # type: ignore
        logger.info("Successfully connected to Azure Blob Storage.")
        return blob_service_client
    except (ConnectionFailure, ConfigurationError) as e:
        logger.error(f"Failed to connect to Azure Blob Storage: {e}")
        exit()


def make_blob_public(container_client, blob_name):
    """
    Sets the access level of a blob to public read.
    """
    try:
        acl = container_client.get_container_access_policy()
        if acl.get('public_access') != 'blob':
            container_client.set_container_access_policy(public_access='blob')
            logger.info(f"Set container access policy to public for blob '{blob_name}'.")
        else:
            logger.info(f"Blob '{blob_name}' is already public.")
    except (KeyError, TypeError, ValueError, AzureError) as e:
        logger.error(f"Failed to set blob '{blob_name}' to public: {e}")


def connect_googlesheet():
    """
    Connects to Book Club DB and returns the spreadsheet.
    """
    try:
        # Google Sheets authorization
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(gsheet_cred, scope)  # type: ignore
        client_sheet = gspread.authorize(creds)

        # Open spreadsheet
        spreadsheet = client_sheet.open("Book Club DB")
        logger.info("Connected to Google Sheet")
        return spreadsheet
    except gspread.exceptions.APIError as e:
        logger.error(f"APIError for sheet 'Book Club DB': {e}")
