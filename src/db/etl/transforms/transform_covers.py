# Import necessary modules
import os
import json
import shutil
from urllib.parse import urlparse
from typing import Tuple
import requests
from loguru import logger
from azure.core.exceptions import AzureError, ResourceNotFoundError
from src.db.utils.connectors import connect_azure_blob
from src.config import COVER_ART_DIR, RAW_COLLECTIONS_DIR, TRANSFORMED_COLLECTIONS_DIR

# Function to delete existing cover art in the local directory
def delete_existing_images():
    """
    Deletes all files from the specified directory
    to clean directory before new images are downloaded.
    """
    logger.info(f"Checking for existing images in '{COVER_ART_DIR}'...")
    try:
        # os.listdir gets all entries in the directory
        files = os.listdir(COVER_ART_DIR)
        if files:
            logger.info(f"Found {len(files)} existing files. Deleting them now...")
            # shutil.rmtree deletes a directory and all its contents
            shutil.rmtree(COVER_ART_DIR)
            # Recreate the directory after deletion
            os.makedirs(COVER_ART_DIR)
            logger.success("Successfully deleted all existing images.")
        else:
            logger.info("Directory is empty. No deletion needed.")
    except OSError as e:
        logger.error(f"Error deleting files from '{COVER_ART_DIR}': {e}")
        # If deletion fails, stop the process to prevent errors later
        exit()


# Connect to Azure Blob Storage
blob_service_client = connect_azure_blob()
CONTAINER_NAME = 'cover-art'


# Download, upload, and update image URLs in Azure Blob Storage
def download_images():
    """
    Loads cover_art.json from disk, downloads each image from its cart_url,
    and saves it to COVER_ART_DIR using the _id as filename.
    """
    input_path = os.path.join(RAW_COLLECTIONS_DIR, "cover_art.json")
    try:
        with open(input_path, encoding="utf-8") as f:
            cover_art_data = json.load(f)
        logger.info(f"Found {len(cover_art_data)} cover art entries to process.")
    except Exception as e:
        logger.error(f"Failed to load cover_art.json: {e}")
        return

    for doc in cover_art_data:
        image_url = doc.get("cart_url")
        if not image_url:
            logger.warning(f"Entry with _id {doc.get('_id')} has no 'cart_url'. Skipping.")
            continue

        try:
            parsed_url = urlparse(image_url)
            extension = os.path.splitext(parsed_url.path)[1] or ".jpg"
            filename = f"{doc['_id']}{extension}"
            file_path = os.path.join(COVER_ART_DIR, filename)

            logger.info(f"Downloading {image_url}...")
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.success(f"Saved image as {filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {image_url}: {e}")
        except IOError as e:
            logger.error(f"Failed to save image {file_path}: {e}")


def delete_container_contents(container_name: str,raise_on_error: bool = True) -> Tuple[bool, int]:
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
            logger.info(f"Successfully deleted {deleted_count} blobs from container '{container_name}'")
        else:
            logger.info(f"Container '{container_name}' was already empty")

        return True, deleted_count

    except AzureError as e:
        error_msg = f"Failed to delete blobs from container '{container_name}': {e}"
        logger.error(error_msg)

        if raise_on_error:
            raise AzureError(error_msg) from e
        return False, deleted_count


def upload_cover_art_files(container_name, source_directory):
    """
    Uploads all image files from a local directory to the specified
    Azure Blob Storage container.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        logger.info(f"Starting upload of files from '{source_directory}' to '{container_name}'...")

        uploaded_count = 0
        for filename in os.listdir(source_directory):
            file_path = os.path.join(source_directory, filename)

            # Check if the file is an image and not a directory
            if os.path.isfile(file_path) and filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                try:
                    # Create a blob client and upload the file
                    blob_client = container_client.get_blob_client(filename)
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
                    logger.debug(f"Uploaded {filename}")
                    uploaded_count += 1
                except (OSError, AzureError) as e:
                    logger.warning(f"Failed to upload {filename}: {e}")

        logger.success(f"Finished uploading. {uploaded_count} files were uploaded to '{container_name}'.")

    except (OSError, AzureError) as e:
        logger.error(f"Failed to upload files to container '{container_name}': {e}")
        # The script should not proceed if this critical step fails
        exit()


def update_image_urls():
    """
    Updates cart_url fields in cover_art.json to point to Azure-hosted URLs.
    """
    input_path = os.path.join(RAW_COLLECTIONS_DIR, "cover_art.json")
    output_path = os.path.join(TRANSFORMED_COLLECTIONS_DIR, "cover_art.json")

    try:
        with open(input_path, encoding="utf-8") as f:
            cover_art_data = json.load(f)
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to load cover_art.json: {e}")
        return

    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blobs = container_client.list_blobs()
    blob_map = {os.path.splitext(blob.name)[0]: blob.name for blob in blobs}

    updated_count = 0
    for doc in cover_art_data:
        object_id_str = str(doc["_id"])
        blob_name = blob_map.get(object_id_str)

        if blob_name:
            new_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{CONTAINER_NAME}/{blob_name}"
            doc["cart_url"] = new_url
            updated_count += 1
            logger.debug(f"Updated cart_url for _id {object_id_str}")
        else:
            logger.warning(f"No blob found for _id {object_id_str}")

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(cover_art_data, f, ensure_ascii=False, indent=2)
        logger.success(f"Updated {updated_count} cart_url entries in cover_art.json")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to write updated cover_art.json: {e}")


if __name__ == "__main__":
    delete_existing_images()
    download_images()
    delete_container_contents(CONTAINER_NAME)
    upload_cover_art_files(CONTAINER_NAME, COVER_ART_DIR)
    update_image_urls()

    logger.info("Transformed cover art collection created.")
