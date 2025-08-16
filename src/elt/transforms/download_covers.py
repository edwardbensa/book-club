# Import modules
import os
from urllib.parse import urlparse
import shutil
import requests
from loguru import logger
from src.elt.transforms.utils import connect_mongodb
from src.config import COVER_ART_DIR

def delete_existing_images():
    """
    Deletes all files from the specified directory.
    This ensures the directory is clean before new images are downloaded.
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
        # If deletion fails, we should stop the process to prevent errors later
        exit()

# Connect to MongoDB
db, client = connect_mongodb()

def download_images():
    """
    Fetches image URLs and cart IDs from the 'cover_art' collection, downloads the images,
    and saves them to a local directory with the cart_id as the filename.
    """
    cover_art_collection = db["cover_art"]
    try:
        # Fetch both cart_url and cart_id
        cover_art_data = list(cover_art_collection.find({}, {"cart_url": 1, "cart_id": 1}))
        logger.info(f"Found {len(cover_art_data)} URLs to download.")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Failed to fetch data from 'cover_art' collection: {e}")
        return

    for doc in cover_art_data:
        image_url = doc.get("cart_url")
        cart_id = doc.get("cart_id")

        if not image_url or not cart_id:
            logger.warning(f"Document with _id {doc.get('_id')} is missing 'cart_url' or 'cart_id'. Skipping.")
            continue

        try:
            # Use the cart_id as the filename and infer the file extension
            parsed_url = urlparse(image_url)
            extension = os.path.splitext(parsed_url.path)[1]
            # If no extension found, default to .jpg
            if not extension:
                extension = ".jpg"

            filename = f"{cart_id}{extension}"
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

if __name__ == "__main__":
    delete_existing_images()
    download_images()
