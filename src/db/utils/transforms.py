"""Transform utility functions"""

# Import modules
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from src.config import RAW_COLLECTIONS_DIR, TRANSFORMED_COLLECTIONS_DIR
from .parsers import clean_document, to_datetime
from .lookups import find_doc


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


def add_rlog(doc: dict):
    """Replaces rstatus_history with reading_log."""
    current_rstatus = doc["current_rstatus_id"]
    if current_rstatus == "rs4":
        doc["reading_log"] = ""
        doc.pop("rstatus_history")
        return doc

    # Find rstatus_history and lh_rstatus
    rstatus_history = doc["rstatus_history"]
    lh_rstatus = ""
    if rstatus_history != "":
        lh_rstatus = str(rstatus_history).rsplit(',', maxsplit=1)[-1].split(":")[0].strip()

    # Amend current_rstatus if current is "Reading" and last historical is "Paused"
    if current_rstatus == "rs2" and lh_rstatus == "rs3":
        current_rstatus = lh_rstatus
        doc["current_rstatus_id"] = current_rstatus

    # Set rstatus_history to now if blank and current_rstatus is "Paused"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if current_rstatus == "rs3" and rstatus_history == "":
        rstatus_history = f"rs3: {now_str}"

    # Set start and end entries
    start = f"rs2: {doc["date_started"]}" if doc["date_started"] != "" else ""
    end = f"rs1: {doc["date_completed"]}" if doc["date_completed"] != "" else ""

    # Set end to now if current_rstatus is "Reading"
    if current_rstatus == "rs2":
        end = f"rs1: {now_str}"

    # Set start to 7 days before now if blank and current_rstatus is "Paused"
    if start == "" and current_rstatus == "rs3":
        start_date = datetime.now() - timedelta(days=21)
        start = f"rs2: {start_date.strftime('%Y-%m-%d')}"

    # Set start to 21 days before end if blank and current_rstatus is "Read"/"Paused"
    if start == "" and current_rstatus in ("rs1", "rs3"):
        completed_date = to_datetime(doc["date_completed"])
        if completed_date is not None:
            start_date = completed_date - timedelta(days=21)
            start = f"rs2: {start_date.strftime("%Y-%m-%d %H:%M:%S")}"

    # Set default start and end if both blank and current_rstatus is "Read"/"Reading"
    if start + end == "" and current_rstatus in ("rs1", "rs2"):
        start = "rs2: 2025-10-10"
        end = "rs1: 2025-10-31"

    # Create reading log
    r_log = f"{start}, {rstatus_history}" if rstatus_history != "" else start
    r_log = f"{r_log}, {end}" if end != "" else r_log

    doc["reading_log"] = r_log
    doc.pop("rstatus_history")
    return doc


def add_d2r(doc):
    """Calculate days to read and add to user_reads."""

    # Skip entries that don't have a reading_log
    r_log = doc.get("reading_log")
    if r_log == "":
        return doc

    # Split into tokens
    tokens = [t.strip() for t in r_log.split(",")]

    # Change last token to "Read" if the rstatus is "Paused"
    last_token = tokens[-1]
    last_status, last_ts = last_token.split(":", 1)
    last_token = f"rs1: {last_ts}" if last_status == "rs3" else last_token

    events = []
    for t in tokens:
        key, value = t.split(":", 1)
        events.append((key.strip(), value.strip()))

    # Collect reading intervals
    intervals = []
    start_time = None

    for key, value in events:
        if key == "rs2":  # start reading
            start_time = to_datetime(value)
        elif key in ("rs3", "rs1"):  # paused or finished
            if start_time:
                end_time = to_datetime(value)
                if end_time:
                    intervals.append(end_time - start_time)
                start_time = None

    d2r = sum((i.total_seconds() / 86400 for i in intervals))
    d2r = 1 if d2r == 0 else d2r
    doc["days_to_read"] = d2r

    return doc

def add_read_rates(doc, book_versions):
    """Calculate reading rates and add to user_reads."""

    # Skip entries that don't have d2r
    d2r = doc.get("days_to_read", None)
    if d2r is None:
        return doc

    version_id = doc.get("version_id")
    bv_doc = find_doc(book_versions, "version_id", version_id)

    # Skip entries that don't have format
    fmt = bv_doc.get("format")
    if fmt == "":
        return doc

    if fmt == "audiobook":
        length = bv_doc.get("length")
        if length == "":
            return doc
        hpd = length / d2r
        doc["hours_per_day"] = hpd
    else:
        pc = bv_doc.get("page_count")
        if pc == "":
            return doc
        ppd = pc / d2r
        doc["pages_per_day"] = ppd

    return doc
