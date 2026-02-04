"""Transform utility functions"""

# Imports
import os
import json
from datetime import datetime
from pathlib import Path
from loguru import logger
from src.config import RAW_COLLECTIONS_DIR, TRANSFORMED_COLLECTIONS_DIR
from .derived_fields import generate_rlog, compute_d2r, compute_rr, find_doc
from .parsers import clean_document


# pylint: disable=line-too-long

def transform_collection(collection_name: str, transform_func):
    """
    Loads a raw JSON collection, transforms each document,
    and writes the result to TRANSFORMED_COLLECTIONS_DIR.
    Assumes _id is already present in the input.
    """
    input_path = os.path.join(RAW_COLLECTIONS_DIR, f"{collection_name}.json")
    output_path = os.path.join(TRANSFORMED_COLLECTIONS_DIR, f"{collection_name}.json")

    try:
        with open(input_path, encoding="utf-8") as f:
            raw_docs = json.load(f)

        transformed = []
        removed_keys = []
        for doc in raw_docs:
            clean_doc, removed = clean_document(transform_func(doc))
            transformed.append(clean_doc)
            removed_keys.extend(removed)

        counts = {item: removed_keys.count(item) for item in sorted(set(removed_keys))}
        if counts != {}:
            logger.warning(f"The following keys were removed: {counts}")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=2)

        logger.info(f"Transformed {len(transformed)} records -> {output_path}")

    except FileNotFoundError:
        logger.warning(f"Raw JSON file not found: {input_path}")
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"Error transforming '{collection_name}': {e}")


def remove_custom_ids(collections_to_cleanup: dict, source_directory):
    """
    Removes specified custom ID fields from each collection in the source directory,
    and writes the cleaned output to TRANSFORMED_COLLECTIONS_DIR.
    """
    source_directory = Path(source_directory)
    output_directory = Path(TRANSFORMED_COLLECTIONS_DIR)

    for collection_name, id_field in collections_to_cleanup.items():
        input_path = source_directory / f"{collection_name}.json"
        output_path = output_directory / f"{collection_name}.json"

        try:
            with input_path.open(encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

            cleaned = []
            for doc in data:
                doc.pop(id_field, None)
                cleaned.append(doc)

            with output_path.open("w", encoding="utf-8") as f:
                json.dump(cleaned, f, ensure_ascii=False, indent=2)

            logger.success(f"Removed '{id_field}' from all documents in '{collection_name}.json'")

        except FileNotFoundError:
            logger.warning(f"File not found: {input_path}")
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to process '{input_path.name}': {e}")


def change_id_field(collections_to_update: dict, source_directory):
    """
    Replaces the _id field in each document with the value from the specified custom ID field.
    Reads from source_directory and writes to TRANSFORMED_COLLECTIONS_DIR.
    
    Args:
        collections_to_update: dict of {collection_name: custom_id_field}
        source_directory: Path or str pointing to the source JSON files
    """
    source_directory = Path(source_directory)
    output_directory = Path(TRANSFORMED_COLLECTIONS_DIR)

    for collection_name, custom_id_field in collections_to_update.items():
        input_path = source_directory / f"{collection_name}.json"
        output_path = output_directory / f"{collection_name}.json"

        try:
            with input_path.open(encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

            updated = []
            for doc in data:
                if custom_id_field in doc:
                    doc["_id"] = str(doc[custom_id_field])
                    del doc[custom_id_field]
                updated.append(doc)

            with output_path.open("w", encoding="utf-8") as f:
                json.dump(updated, f, ensure_ascii=False, indent=2)

            logger.success(f"Replaced _id with '{custom_id_field}' in all documents of '{output_path.name}'")

        except FileNotFoundError:
            logger.warning(f"File not found: {input_path}")
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to process '{input_path.name}': {e}")


def remove_document_by_index(collection_name: str, source_directory, index: int):
    """
    Removes the document at the specified 1-based index from a JSON collection.
    Saves the result to TRANSFORMED_COLLECTIONS_DIR.

    Args:
        collection_name: name of the collection (e.g. 'books')
        source_directory: path to the folder containing the source JSON file
        index: 1-based index of the document to remove
    """
    source_directory = Path(source_directory)
    output_directory = Path(TRANSFORMED_COLLECTIONS_DIR)

    input_path = source_directory / f"{collection_name}.json"
    output_path = output_directory / f"{collection_name}.json"

    try:
        with input_path.open(encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

        if index < 1 or index > len(data):
            logger.warning(f"Index {index} is out of bounds for '{collection_name}'")
            return

        removed_doc = data.pop(index - 1)
        removed_id = removed_doc.get("_id", "unknown")
        logger.success(f"Removed document at index {index} (id: {removed_id}) from '{collection_name}'")

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved updated collection to '{output_path.name}'")

    except FileNotFoundError:
        logger.error(f"File not found: {input_path}")
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to process '{input_path.name}': {e}")


def remove_documents_by_field(collection_name: str, source_directory, field_name: str, field_value):
    """
    Removes all documents from a JSON collection where field_name == field_value.
    Saves the result to TRANSFORMED_COLLECTIONS_DIR.

    Args:
        collection_name: name of the collection
        source_directory: path to the folder containing the source JSON file
        field_name: the field to match
        field_value: the value to match and remove
    """
    source_directory = Path(source_directory)
    output_directory = Path(TRANSFORMED_COLLECTIONS_DIR)

    input_path = source_directory / f"{collection_name}.json"
    output_path = output_directory / f"{collection_name}.json"

    try:
        with input_path.open(encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} documents from '{input_path.name}'")

        filtered = [doc for doc in data if doc.get(field_name) != field_value]
        removed_count = len(data) - len(filtered)

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2)

        logger.success(f"Removed {removed_count} documents from '{collection_name}' where {field_name} == {field_value}")
        logger.info(f"Saved {len(filtered)} remaining documents to '{output_path.name}'")

    except FileNotFoundError:
        logger.error(f"File not found: {input_path}")
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to process '{input_path.name}': {e}")


def add_timestamp(collection_name: str):
    """
    Adds a 'date_added' field to each document in the specified collection.
    """
    directory = Path(TRANSFORMED_COLLECTIONS_DIR)
    file_path = directory / f"{collection_name}.json"

    try:
        with file_path.open(encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} documents from '{file_path.name}'")

        now_str = str(datetime.now())
        for doc in data:
            doc["date_added"] = now_str

        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.success(f"Added 'date_added' to all documents in '{collection_name}'")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.error(f"Failed to process '{file_path.name}': {e}")


def add_read_details(doc, book_versions):
    """Add reading log, days to read, and read rates."""

    # Skip if current_rstatus is "To Read"
    current_rstatus = doc["rstatus_id"]
    if current_rstatus == "rs4":
        doc["reading_log"] = ""
        doc.pop("rstatus_history")
        return doc

    version_id = doc.get("version_id")
    version_doc = find_doc(book_versions, "version_id", version_id)

    # Add reading log
    doc["reading_log"] = generate_rlog(doc)
    logger.info(f"Reading log generated for {version_doc["title"]}.")

    # Add days to read
    doc["days_to_read"] = compute_d2r(doc)
    logger.info(f"D2R computed for {version_doc["title"]}.")

    # Add read rate
    metric = "hours" if version_doc["format"] == "audiobook" else "pages"
    doc[f"{metric}_per_day"] = compute_rr(doc, book_versions)

    doc.pop("rstatus_history")
    return doc
