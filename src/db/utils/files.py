"""File management utility functions"""

# Imports
import os
import json
import shutil
import hashlib
from datetime import datetime
from urllib.parse import urlparse
from loguru import logger
import requests
from src.config import RAW_COLLECTIONS_DIR, ETL_LOGS_DIR


def wipe_directory(directory):
    """
    Deletes all files from specified directory.
    """
    logger.info(f"Checking for existing files in '{directory}'...")
    try:
        files = os.listdir(directory)
        if files:
            logger.info(f"Found {len(files)} existing files. Deleting them now...")
            shutil.rmtree(directory)
            os.makedirs(directory)
            logger.success("Successfully deleted all existing files.")
        else:
            logger.info("Directory is empty. No deletion needed.")
    except OSError as e:
        logger.error(f"Error deleting files from '{directory}': {e}")
        exit()


def generate_image_filename(doc: dict, img_type: str):
    """
    Generate a hashed filename for a profile image using a unique field entry.
    Also generates a hashed filename for a book cover using the first valid unique identifier
    where prefix is based on the index of the identifier in the list.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    hash_length=20
    if img_type == "cover":
        unique_fields=['isbn_13', 'asin']
        for index, unique_field in enumerate(unique_fields):
            value = str(doc.get(unique_field))
            if value and isinstance(value, str) and value.strip():
                hash_digest = hashlib.sha256(value.encode()).hexdigest()[:hash_length]
                prefix = f"b{str(index + 1).zfill(2)}"
                return f"{prefix}-{hash_digest}"

        raise ValueError("No valid unique identifier found in book metadata.")
    elif img_type in ["user", "club"]:
        field = doc.get(f"{img_type}_handle")
        if field and isinstance(field, str) and field.strip():
            hash_digest = hashlib.sha256(field.encode()).hexdigest()[:hash_length]
            return f"{hash_digest}.jpg"
    else:
        field = doc.get("profile_photo")
        if field and isinstance(field, str) and field.strip():
            hash_digest = hashlib.sha256(field.encode()).hexdigest()[:hash_length]
            return f"{hash_digest}.jpg"

        raise ValueError("Missing or invalid handle for profile photo.")


def download_images(collection_name, url_field, img_type, output_directory):
    """
    Downloads all image files from URLs specified in json file.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    raw_file = f"{collection_name}.json"
    input_path = os.path.join(RAW_COLLECTIONS_DIR, raw_file)

    try:
        with open(input_path, encoding="utf-8") as f:
            input_file_data = json.load(f)
        logger.info(f"Found {len(input_file_data)} entries to process.")
    except (KeyError, json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to load {raw_file}: {e}")
        return

    os.makedirs(ETL_LOGS_DIR, exist_ok=True)
    log_filename = f"{img_type}s_imagefiles_log.json"
    log_path = os.path.join(ETL_LOGS_DIR, log_filename)

    # Load previous log if exists
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            log_data = json.load(f)
    else:
        log_data = []

    files_in_directory = os.listdir(output_directory) if os.path.exists(output_directory) else []
    last_run_id = log_data[-1]["run_id"] if log_data else 0
    filenames = []
    for doc in input_file_data:
        image_url = doc.get(url_field)
        if not image_url:
            logger.warning(f"Entry with _id {doc.get('_id')} has no {url_field}. Skipping.")
            continue

        try:
            parsed_url = urlparse(image_url)
            extension = os.path.splitext(parsed_url.path)[1] or ".jpg"
            filename = f"{generate_image_filename(doc, img_type)}{extension}"
            file_path = os.path.join(output_directory, filename)

            if filename in files_in_directory:
                logger.info(f"File {filename} already exists. Skipping download.")
                filenames.append(filename)
                continue

            logger.info(f"Downloading {image_url}...")
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            filenames.append(filename)
            logger.success(f"Saved image as {filename}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {image_url}: {e}")
        except IOError as e:
            logger.error(f"Failed to save image {file_path}: {e}")

    # Create new log entry
    new_entry = {
        "run_id": last_run_id + 1,
        "timestamp": datetime.now().isoformat(),
        "filenames": filenames
    }

    # Append and save
    log_data.append(new_entry)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)
    logger.info(f"Logged {len(filenames)} filenames to {log_path}")


def selective_delete(directory, img_type):
    """
    Deletes files from directory that are not listed in the latest log entry.
    """
    if img_type not in ["user", "club", "cover", "creator"]:
        raise ValueError("Type must be either 'user', 'club', or 'cover'")

    log_filename = f"{img_type}s_imagefiles_log.json"
    log_path = os.path.join(ETL_LOGS_DIR, log_filename)

    if not os.path.exists(log_path):
        logger.warning(f"No log file found at {log_path}. Skipping selective deletion.")
        return

    with open(log_path, "r", encoding="utf-8") as f:
        log_data = json.load(f)

    if not log_data:
        logger.warning(f"Log file at {log_path} is empty. Skipping selective deletion.")
        return

    latest_entry = log_data[-1]
    valid_filenames = set(latest_entry.get("filenames", []))

    try:
        files_in_directory = os.listdir(directory)
        files_to_delete = [f for f in files_in_directory if f not in valid_filenames]

        if not files_to_delete:
            logger.info("No files to delete. Directory is up to date.")
            return

        for filename in files_to_delete:
            file_path = os.path.join(directory, filename)
            os.remove(file_path)
            logger.info(f"Deleted obsolete file: {filename}")

        logger.success(f"Deleted {len(files_to_delete)} obsolete files from {directory}.")

    except OSError as e:
        logger.error(f"Error during selective deletion in '{directory}': {e}")
