# Import necessary modules
import os
from urllib.parse import urlparse
import shutil
import requests
from loguru import logger
from bson.objectid import ObjectId
from azure.core.exceptions import AzureError
from src.elt.transforms.utils import connect_mongodb, connect_azure_blob
from src.config import COVER_ART_DIR

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

# --- 1. SET UP CONNECTIONS AND CLIENTS ---

# Connect to MongoDB
db, client = connect_mongodb()

# Connect to Azure Blob Storage
blob_service_client = connect_azure_blob()

# Define the container name where you upload and retrieve images
CONTAINER_NAME = "cover-art"

# Download, upload, and update image URLs in Azure Blob Storage

def download_images():
    """
    Fetches image URLs from the 'cover_art' collection, downloads the images,
    and saves them to a local directory.
    """
    cover_art_collection = db["cover_art"]
    try:
        cover_art_data = list(cover_art_collection.find({}, {"cart_url": 1}))
        logger.info(f"Found {len(cover_art_data)} URLs to download.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'cover_art' collection: {e}")
        return

    for doc in cover_art_data:
        image_url = doc.get("cart_url")
        if not image_url:
            logger.warning(f"Document with _id {doc.get('_id')} has no 'cart_url'. Skipping.")
            continue

        try:
            # Use the cart_id as the filename and infer the file extension
            parsed_url = urlparse(image_url)
            extension = os.path.splitext(parsed_url.path)[1]
            # If no extension found, default to .jpg
            if not extension:
                extension = ".jpg"

            filename = f"{str(doc.get('_id'))}{extension}"
            file_path = os.path.join(COVER_ART_DIR, filename)

            logger.info(f"Downloading {image_url}...")
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()  # Raise exception for bad status codes

            # Save the image content to the local file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.success(f"Successfully saved {filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download image from {image_url}: {e}")
        except IOError as e:
            logger.error(f"Failed to save file {file_path}: {e}")

def delete_container_contents(container_name):
    """
    Deletes all blobs from the specified Azure Blob Storage container.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs_to_delete = container_client.list_blobs()

        logger.info(f"Checking for existing blobs in container '{container_name}'...")

        # Count existing blobs to decide whether to proceed
        blob_count = sum(1 for _ in blobs_to_delete)
        if blob_count > 0:
            logger.info(f"Found {blob_count} blobs. Deleting them now...")
            for blob in blobs_to_delete:
                container_client.delete_blob(blob.name)
                logger.debug(f"Deleted blob: {blob.name}")
            logger.success(f"Successfully deleted all blobs from container '{container_name}'.")
        else:
            logger.info(f"Container '{container_name}' is already empty. No deletion needed.")

    except AzureError as e:
        logger.error(f"Failed to delete blobs from container '{container_name}': {e}")
        # The script should not proceed if this critical step fails
        exit()

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
    Connects to Azure Blob Storage, retrieves the URLs of uploaded images,
    and updates the corresponding documents in the MongoDB 'cover_art' collection.
    """
    try:
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        # List all blobs (files) in the container
        blobs = container_client.list_blobs()
        logger.info("Listing blobs in Azure container to update MongoDB URLs...")
    except (AzureError, OSError) as e:
        logger.error(f"Failed to get container client or list blobs: {e}")
        return

    updated_count = 0
    for blob in blobs:
        object_id_str = os.path.splitext(blob.name)[0]

        try:
            # Construct the new public URL for the image
            new_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{CONTAINER_NAME}/{blob.name}"

            # Find the document in MongoDB by its ObjectId and update the 'cart_url' field
            result = db["cover_art"].update_one(
                {"_id": ObjectId(object_id_str)},
                {"$set": {"cart_url": new_url}}
            )

            if result.matched_count > 0:
                logger.success(f"Updated document with _id {object_id_str}. New URL: {new_url}")
                updated_count += 1
            else:
                logger.warning(f"No document found for _id: {object_id_str}. This may indicate a mismatch between files and database entries.")

        except (AzureError, OSError, ValueError, TypeError) as e:
            logger.error(f"Failed to update URL for blob {blob.name}: {e}")
            continue

    logger.success(f"Finished updating MongoDB. {updated_count} documents were modified.")

# --- 3. MAIN EXECUTION BLOCK ---

if __name__ == "__main__":
    delete_existing_images()
    download_images()
    delete_container_contents(CONTAINER_NAME)
    upload_cover_art_files(CONTAINER_NAME, COVER_ART_DIR)
    update_image_urls()

    client.close()
    logger.info("MongoDB connection closed.")
