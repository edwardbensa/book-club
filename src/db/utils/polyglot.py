"""MongoDB -> AuraDB polyglot persistence utility functions"""

# Imports
from collections import defaultdict
from datetime import datetime
from loguru import logger
from bson import ObjectId


def safe_value(v):
    """Convert ObjectIds to strings and datetime to ISO format."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, str):
        return v
    return v

def remove_nested_dicts(entry):
    """Remove keys whose values are dicts or lists of dicts."""
    for key in list(entry.keys()):
        value = entry[key]

        if isinstance(value, dict):
            entry.pop(key)
            continue

        if isinstance(value, list) and any(isinstance(i, dict) for i in value):
            entry.pop(key)

    return entry


def flatten_document(entry, field_map):
    """Flatten document using a field map."""
    entry = entry.copy()
    output_lists = defaultdict(list)

    for out_field, path in field_map.items():
        parent, child = path.split(".")
        value = entry.get(parent, [])

        # Parent is a dict
        if isinstance(value, dict):
            if child in value:
                output_lists[out_field].append(safe_value(value[child]))

        # Parent is a list of dicts
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and child in item:
                    output_lists[out_field].append(safe_value(item[child]))

    # Insert flattened fields and remove nested dicts
    entry.update(output_lists)
    reassigned = [k for k, v in field_map.items() if k == v.split(".")[0]]
    field_map = {k: v for k, v in field_map.items() if v.split(".")[0] not in reassigned}
    for path in field_map.values():
        if "." in str(path):
            parent = path.split(".")[0]
            entry.pop(parent, None)

    return entry


def fetch_from_mongo(collection, exclude_fields=None, field_map=None):
    """
    Fetch collection from MongoDB with field exclusions
    Flatten nested dicts
    """
    if exclude_fields is None:
        exclude_fields = []
    if field_map is None:
        field_map = {}

    projection = {field: 0 for field in exclude_fields}
    docs = list(collection.find({}, projection))

    flattened = []
    for doc in docs:
        doc = {k: safe_value(v) for k, v in doc.items()}

        # Flatten
        flat = flatten_document(doc, field_map)
        flattened.append(flat)
    logger.success(f"Fetched {len(flattened)} documents from {collection.name} collection.")

    return flattened


def upsert_nodes(tx, label, rows, id_field="_id"):
    """Generic AuraDB upsert function"""
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{_id: row.{id_field}}})
    SET n += row
    """
    tx.run(query, rows=rows)
    logger.success(f"Upserted {len(rows)} '{label}' nodes into Neo4j database.")


def clear_all_nodes(driver):
    """Clear all nodes in graph."""
    query = "MATCH (n) DETACH DELETE n"
    with driver.session() as session:
        session.run(query)


def create_relationships(tx, rel_map, rel: str):
    """
    Create relationships between two sets of nodes in AuraDB.
    
    Args:
        tx: Neo4j session
        rel_map: Dict of labels [source, target] and properties [source, target]
        relationship_type: Type of relationship to create (e.g., 'HAS_GENRE')
    
    Returns:
        Number of relationships created
    """
    # Extract node labels and fields
    source_label = rel_map["labels"][0]
    target_label = rel_map["labels"][1]
    source_prop = rel_map["props"][0]
    target_prop = rel_map["props"][1]

    query = f"""
    MATCH (source:{source_label})
    WHERE source.{source_prop} IS NOT NULL
    UNWIND source.{source_prop} AS value
    MATCH (target:{target_label} {{{target_prop}: value}})
    MERGE (source)-[:{rel}]->(target)
    RETURN count(*) AS relationships_created
    """

    result = tx.run(query)
    count = result.single()["relationships_created"]
    logger.info(f"Created {count} relationships of type {rel}")


def user_reads_relationships(tx, user_reads):
    """
    Create relationships between users and the books they've read.
    """

    rel_map = {
        "DNF": "DID_NOT_FINISH",
        "Read": "HAS_READ",
        "Paused": "HAS_PAUSED",
        "Reading": "IS_READING",
        "To Read": "WANTS_TO_READ"
    }

    query = """
    MATCH (u:User {_id: $user_id})
    MATCH (b:BookVersion {_id: $version_id})
    MERGE (u)-[r:%s]->(b)
    SET r.rating = $rating,
        r.days_to_read = $days_to_read,
        r.read_rate = $read_rate
    """

    for doc in user_reads:
        rel_type = rel_map.get(doc.get("current_rstatus"))
        if not rel_type:
            continue

        read_rate = doc.get("read_rate_pages") or doc.get("read_rate_hours")

        tx.run(
            query % rel_type,
            user_id=doc.get("user_id"),
            version_id=doc.get("version_id"),
            rating=doc.get("rating"),
            days_to_read=doc.get("days_to_read"),
            read_rate=read_rate
        )

    logger.info("Created relationships.")

def user_reads_relationships2(tx, user_reads):
    """
    Create relationships between users and the books they've read.
    """
    rel_map = {
        "DNF": "DID_NOT_FINISH",
        "Read": "HAS_READ",
        "Paused": "HAS_PAUSED",
        "Reading": "IS_READING",
        "To Read": "WANTS_TO_READ"
    }

    rows = []
    for doc in user_reads:
        rel_type = rel_map.get(doc.get("current_rstatus"))
        if not rel_type:
            continue

        rows.append({
            "user_id": doc.get("user_id"),
            "book_id": doc.get("book_id"),
            "rating": doc.get("rating"),
            "days_to_read": doc.get("days_to_read"),
            "read_rate": doc.get("read_rate_pages") or doc.get("read_rate_hours"),
            "rel_type": rel_type
        })

    query = """
    UNWIND $rows AS row
    MATCH (u:User {_id: row.user_id})
    MATCH (b:Book {_id: row.book_id})
    CALL apoc.merge.relationship(u, row.rel_type, {}, {}, b) YIELD rel
    SET rel.rating = row.rating,
        rel.days_to_read = row.days_to_read,
        rel.read_rate = row.read_rate
    """

    tx.run(query, rows=rows)

    logger.info(f"Created or updated {len(rows)} relationships.")

