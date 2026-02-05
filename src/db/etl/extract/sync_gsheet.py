"""Google Sheets sync: updates, adds, and removes records while preserving ObjectIds"""

# Imports
import os
import json
import time
import copy
import hashlib
from loguru import logger
from bson.objectid import ObjectId
from src.db.utils.connectors import connect_googlesheet
from src.config import RAW_COLLECTIONS_DIR

# Connect to Book Club DB spreadsheet
spreadsheet = connect_googlesheet()

# Define unique id maps to check when updating data
id_map = {
    "books": ["title", "genre"],
    "book_versions": ["isbn_13", "asin"],
    "creators": ["firstname", "lastname"],
    "creator_roles": ["name"],
    "genres": ["name"],
    "book_series": ["name"],
    "awards": ["name"],
    "publishers": ["name"],
    "formats": ["name"],
    "tags": ["name"],
    "languages": ["name"],
    "users": ["handle"],
    "user_reads": [],
    "read_statuses": ["name"],
    "user_badges": ["name"],
    "user_roles": ["name"],
    "user_permissions": ["name"],
    "clubs": ["handle"],
    "club_members": ["club_id", "user_id"],
    "club_member_reads": ["club_id", "user_id", "book_id"],
    "club_reading_periods": ["club_id", "period_id"],
    "club_period_books": ["club_id", "period_id", "book_id"],
    "club_discussions": ["club_id", "discussion_id"],
    "club_events": ["club_id", "name"],
    "club_event_types": ["name"],
    "club_event_statuses": ["name"],
    "club_badges": ["name"],
    "countries": ["name"]
}

# Sync sheets and update JSON with changes
def hash_doc(doc:dict):
    """Hash a dict."""
    hsh = hashlib.sha1(json.dumps(doc, sort_keys=True).encode()).hexdigest()

    return hsh

def add_hashes(documents: list, sheet_name):
    """Adds hash to each doc by hashing unique identifiers."""
    copy_1 = copy.deepcopy(documents)
    copy_2 = copy.deepcopy(documents)

    identifier_fields = id_map[sheet_name]
    hashes = []

    for doc, d_doc in zip(copy_1, copy_2):
        doc.pop("hash", None)
        d_doc.pop("hash", None)
        d_doc.pop("_id", None)

        hsh = hash_doc(d_doc)
        if identifier_fields != []:
            subdoc = {}
            for k in identifier_fields:
                subdoc[k] = d_doc[k]
            hsh = hash_doc(subdoc)

        doc["hash"] = hsh
        hashes.append(hsh)

    return copy_1, hashes

def update_records(old_documents, new_documents):
    """Return updated docs and structured diff of changes."""

    new_lookup = {doc["hash"]: doc for doc in new_documents}

    updated_docs = []
    diff = {
        "updated": [],
        "unchanged": []
    }

    for old_doc in old_documents:
        old_hash = old_doc["hash"]
        new_doc = new_lookup.get(old_hash)

        if not new_doc:
            diff["unchanged"].append(old_doc)
            updated_docs.append(old_doc)
            continue

        changes = {}
        for key, new_value in new_doc.items():
            if key == "_id":
                continue
            old_value = old_doc.get(key)
            if old_value != new_value:
                changes[key] = {"from": old_value, "to": new_value}
                old_doc[key] = new_value

        if changes:
            diff["updated"].append({
                "_id": old_doc["_id"],
                "hash": old_hash,
                "before": {k: v["from"] for k, v in changes.items()},
                "after": {k: v["to"] for k, v in changes.items()},
                "changes": changes
            })
        else:
            diff["unchanged"].append(old_doc)

        updated_docs.append(old_doc)

    return updated_docs, diff

def cleanup(documents:list):
    """Adds ObjectIds to docs that don't have one and removes hashes."""
    new_documents = []
    for doc in documents:
        if doc.get("_id", None) is None:
            new_doc = {"_id": str(ObjectId())}
            new_doc.update(doc)
            new_doc.pop("hash", None)
            new_documents.append(new_doc)
        else:
            doc.pop("hash", None)
            new_documents.append(doc)

    return new_documents


def sync_sheet(sheet_names):
    """Sync GSheets"""

    if spreadsheet is None:
        logger.error("Failed to connect to Google Sheets")
        return

    for name in sheet_names:
        sheet = spreadsheet.worksheet(name)
        new_list = sheet.get_all_records()

        if new_list:
            # Load old docs if present
            output_path = os.path.join(RAW_COLLECTIONS_DIR, f"{name}.json")
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    old_list = json.load(f)
                logger.info(f"Found {len(old_list)} stored records for '{name}'.")
            except FileNotFoundError:
                logger.info(f"No stored records found for '{name}'. Saving as new.")
                new_list = [{"_id": str(ObjectId()), **i} for i in new_list]
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(new_list, f, ensure_ascii=False, indent=2)
                logger.info(f"Saved {len(new_list)} records to '{output_path}'")
                continue
        else:
            logger.warning(f"No records found in sheet '{name}'")

        # Add hashes to list entries
        new_list, new_hashes = add_hashes(new_list, name)
        old_list, old_hashes = add_hashes(old_list, name)

        # Remove entries whose unique identifier hash isn't in the new hash list
        records = [i for i in old_list if i["hash"] in new_hashes]
        removed_entries = [i for i in old_list if i["hash"] not in new_hashes]

        # Update existing records
        records, update_diff = update_records(records, new_list)

        # Add new entries
        new_entries = [i for i in new_list if i["hash"] not in old_hashes]
        records = records + new_entries

        # Diff and sync summary
        full_diff = {
            "added": new_entries,
            "removed": removed_entries,
            "updated": update_diff["updated"],
            "unchanged": update_diff["unchanged"]
        }
        summary = {
            "added": len(full_diff["added"]),
            "removed": len(full_diff["removed"]),
            "updated": len(full_diff["updated"])
        }
        logger.info(
            f"{name}: {summary['added']} added, "
            f"{summary['removed']} removed, "
            f"{summary['updated']} updated."
            )

        # Preserve ObjectIds and remove hashes
        records = cleanup(records)

        # Save
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)


# Sheet groups
book_sheets = [
    "books", "book_versions", "creators", "creator_roles", "genres", "book_series",
    "awards", "publishers", "formats", "tags", "languages"
]

user_sheets = [
    "users", "user_reads", "read_statuses", "user_badges", "user_roles", "user_permissions"
]

club_sheets = [
    "clubs", "club_members", "club_member_reads", "club_reading_periods", "club_period_books",
    "club_discussions", "club_events", "club_event_types", "club_event_statuses", "club_badges"
]

other = ["countries"]

# Run extraction
if __name__ == "__main__":
    sync_sheet(book_sheets)
    time.sleep(10)
    sync_sheet(user_sheets)
    time.sleep(10)
    sync_sheet(club_sheets)
    sync_sheet(other)
    logger.success("All raw collections saved to disk.")
