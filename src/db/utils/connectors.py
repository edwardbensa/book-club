# Import modules
import os
import json
from typing import Tuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ConfigurationError
from loguru import logger
from dotenv import load_dotenv
from src.config import ETL_LOGS_DIR

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

blobserviceclient_account_name = connect_azure_blob().account_name if azure_storage_connection_string else None

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


def wipe_container(blob_service_client, container_name: str,
                   raise_on_error: bool = True) -> Tuple[bool, int]:
    """
    Deletes all blobs from the specified Azure Blob Storage container.
    
    Args:
        container_name: Name of the container to clear
        raise_on_error: Whether to raise exceptions or return error status
        
    Returns:
        Tuple of (success: bool, deleted_count: int)
        
    Raises:
        AzureError: If raise_on_error is True and deletion fails
        ValueError: If container_name is empty or None
    """
    if not container_name or not container_name.strip():
        raise ValueError("Container name cannot be empty or None")

    deleted_count = 0

    try:
        container_client = blob_service_client.get_container_client(container_name)

        # Check if container exists
        if not container_client.exists():
            logger.warning(f"Container '{container_name}' does not exist")
            return False, 0

        logger.info(f"Starting deletion of blobs from container '{container_name}'...")

        # Get blob list iterator (more memory efficient)
        blob_pages = container_client.list_blobs().by_page()

        for page in blob_pages:
            batch_blobs = list(page)
            if not batch_blobs:
                break

            logger.debug(f"Processing batch of {len(batch_blobs)} blobs")

            # Delete blobs in current batch
            for blob in batch_blobs:
                try:
                    container_client.delete_blob(blob.name, delete_snapshots="include")
                    deleted_count += 1
                    logger.debug(f"Deleted blob: {blob.name}")
                except ResourceNotFoundError:
                    # Blob might have been deleted by another process
                    logger.debug(f"Blob already deleted: {blob.name}")
                except AzureError as blob_error:
                    logger.error(f"Failed to delete blob '{blob.name}': {blob_error}")
                    if raise_on_error:
                        raise

        if deleted_count > 0:
            logger.info(f"Successfully deleted {deleted_count} blobs from '{container_name}'")
        else:
            logger.info(f"Container '{container_name}' was already empty")

        return True, deleted_count

    except AzureError as e:
        error_msg = f"Failed to delete blobs from container '{container_name}': {e}"
        logger.error(error_msg)

        if raise_on_error:
            raise AzureError(error_msg) from e
        return False, deleted_count


def sync_images(blob_service_client, container_name, source_directory, img_type):
    """
    Synchronizes image files between a local directory and an Azure Blob Storage container.
    Uploads new files and deletes obsolete files based on log comparisons.
    """
    if img_type not in ["user", "club", "cover"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    log_filename = f"{img_type}s_imagefiles_log.json"
    log_path = os.path.join(ETL_LOGS_DIR, log_filename)

    # Load latest log entry
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            log_data = json.load(f)
            latest_filenames = set(log_data[-1]["filenames"]) if log_data else set()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load log file: {e}")
        return

    try:
        container_client = blob_service_client.get_container_client(container_name)
        logger.info(f"Syncing container '{container_name}' with local directory '{source_directory}'...")

        # List existing blobs in container
        existing_blobs = set(blob.name for blob in container_client.list_blobs())

        # Delete blobs not in latest log
        blobs_to_delete = [name for name in existing_blobs if name not in latest_filenames]
        for filename in blobs_to_delete:
            try:
                blob_client = container_client.get_blob_client(filename)
                blob_client.delete_blob()
                logger.debug(f"Deleted obsolete blob: {filename}")
            except AzureError as e:
                logger.warning(f"Failed to delete blob {filename}: {e}")

        # Upload files in latest log that aren't in container
        files_to_upload = [name for name in latest_filenames if name not in existing_blobs]
        for filename in files_to_upload:
            file_path = os.path.join(source_directory, filename)
            if os.path.isfile(file_path) and filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                try:
                    blob_client = container_client.get_blob_client(filename)
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                    logger.debug(f"Uploaded: {filename}")
                except (OSError, AzureError) as e:
                    logger.warning(f"Failed to upload {filename}: {e}")
            else:
                logger.debug(f"Skipped non-image or missing file: {filename}")

        logger.success(f"Sync complete. {len(files_to_upload)} uploaded, {len(blobs_to_delete)} deleted.")

    except AzureError as e:
        logger.error(f"Critical error accessing container '{container_name}': {e}")
        exit()
