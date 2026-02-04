"""MongoDB -> AuraDB polyglot persistence utility functions"""

# Imports
from collections import defaultdict
from datetime import datetime
from loguru import logger
from bson import ObjectId
from .embedding import vectorise_text


def safe_value(v):
    """Convert ObjectIds to strings and datetime to ISO format."""
    if isinstance(v, list):
        return [safe_value(item) for item in v]

    if isinstance(v, dict):
        return {k: safe_value(val) for k, val in v.items()}

    if isinstance(v, ObjectId):
        return str(v)

    if isinstance(v, datetime):
        return v.isoformat()

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


def fetch_club_period_books(db):
    """Find the books selected by clubs to read and the selection periods."""
    cpb = fetch_from_mongo(db["club_period_books"])
    crp = fetch_from_mongo(db["club_reading_periods"])

    for i in cpb:
        i["period_name"] = [k["name"] for k in crp if k["_id"] == i["period_id"]][0]

    return cpb


def process_books(books):
    """Convert book award data to str and embed descriptions."""

    # Generate list of dicts with book award data
    book_awards = []

    for book in books:
        book_id = book.get("_id")
        awards = book.get("awards", [])

        for award in awards:
            book_awards.append({
                "book_id": book_id,
                "award_id": award.get("_id"),
                "award_name": award.get("name", ""),
                "award_category": award.get("category", ""),
                "award_year": award.get("year"),
                "award_status": award.get("status", "")
            })

    # Convert book awards list to dict by concatenating list members
    ba_map = {}
    u_ids = set(i["book_id"] for i in book_awards)

    for _id in u_ids:
        awards_list = []
        award_docs = [i for i in book_awards if i["book_id"] == _id]

        for ad in award_docs:
            award = ad["award_name"]
            if ad["award_category"] != "":
                award = f"{ad["award_name"]} for {ad["award_category"]}"
            awards_list.append(f"{award}, {ad["award_year"]}, {ad["award_status"]}")

        str_awards = "; ".join(str(i) for i in awards_list)
        ba_map[_id] = str_awards

    # Replace awards entries in books with new awards strings
    for book in books:
        book["awards"] = ba_map.get(book["_id"], None)
        if book["awards"] is None:
            book.pop("awards")

    # Enrich and embed book descriptions
    for book in books:
        try:
            combo = f"Title: {book["title"]}\
                \n\nAuthor: {", ".join(str(k) for k in book["author"])}\
                \n\nGenres: {", ".join(str(k) for k in book["genre"])}\
                \n\nDescription: {book["description"]}"
            book["description_embedding"] = vectorise_text(combo)
        except KeyError:
            logger.warning(f"Description not found for {book["title"]}")
            continue

    return books, book_awards


def agg_user_reads(user_reads):
    """
    Aggregate user/version reading entries to obtain:
    - most recent rstatus
    - most recent start date
    - most recent read date
    - read count
    - avg rating
    - avg days to read
    - avg read rate (pages_per_day or hours_per_day)
    """
    agg_ur = []
    version_ids = set(i["version_id"] for i in user_reads)
    user_ids = set(i["user_id"] for i in user_reads)
    priority = {"Read": 3, "Reading": 2, "Paused": 1, "To Read": 0}

    for u_id in user_ids:
        for v_id in version_ids:

            # All entries for this user + version
            entries = [
                e for e in user_reads
                if e["user_id"] == u_id and e["version_id"] == v_id
            ]
            if not entries:
                continue

            # Flatten reading logs
            logs = []
            for e in entries:
                if "reading_log" in e and e["reading_log"]:
                    logs.extend(e["reading_log"])

            if logs:
                # Most recent rstatus
                most_recent_event = max(logs, key=lambda x:
                                        (x["timestamp"], priority.get(x["rstatus"], -1)))
                most_recent_rstatus = most_recent_event["rstatus"]

                # Most recent start ("Reading")
                reading_events = [l for l in logs if l["rstatus"] == "Reading"]
                most_recent_start = (
                    max(reading_events, key=lambda x: x["timestamp"])["timestamp"]
                    if reading_events else None
                )

                # Most recent read ("Read")
                read_events = [l for l in logs if l["rstatus"] == "Read"]
                most_recent_read = (
                    max(read_events, key=lambda x: x["timestamp"])["timestamp"]
                    if read_events else None
                )

                read_count = len(read_events)

            else:
                # no logs likely means "To Read"
                most_recent_rstatus = "To Read"
                most_recent_start = None
                most_recent_read = None
                read_count = 0

            # Most recent review
            try:
                most_recent_review = [i["notes"] for i in entries][0]
            except KeyError:
                most_recent_review = None

            # Averages
            ratings = [e.get("rating") for e in entries if e.get("rating") is not None]
            avg_rating = sum(ratings) / len(ratings) if ratings else None

            d2r = [e.get("days_to_read") for e in entries if e.get("days_to_read")]
            avg_days_to_read = sum(d2r) / len(d2r) if d2r else None

            rates = []
            for e in entries:
                if "pages_per_day" in e and e["pages_per_day"]:
                    rates.append(e["pages_per_day"])
                elif "hours_per_day" in e and e["hours_per_day"]:
                    rates.append(e["hours_per_day"])
            avg_read_rate = sum(rates) / len(rates) if rates else None

            agg_ur.append({
                "user_id": u_id,
                "version_id": v_id,
                "most_recent_rstatus": most_recent_rstatus,
                "most_recent_start": most_recent_start,
                "most_recent_read": most_recent_read,
                "most_recent_review": most_recent_review,
                "read_count": read_count,
                "avg_rating": avg_rating,
                "avg_days_to_read": avg_days_to_read,
                "avg_read_rate": avg_read_rate,
            })

    return agg_ur


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


def cleanup_nodes(driver, label_props: dict, batch_size: int = 5000):
    """
    Remove specified properties from nodes of given labels, safely and in batches.
    """
    for label, props in label_props.items():
        logger.info(f"Cleaning up {label} nodes")

        total_nodes_touched = 0
        removal_counts = {prop: 0 for prop in props}

        for prop in props:
            while True:
                query = f"""
                MATCH (n:{label})
                WHERE n.{prop} IS NOT NULL
                WITH n LIMIT $batch_size
                REMOVE n.{prop}
                RETURN count(n) AS removed
                """

                with driver.session() as session:
                    result = session.run(query, batch_size=batch_size)
                    removed = result.single()["removed"]

                if removed == 0:
                    break

                removal_counts[prop] += removed
                total_nodes_touched += removed

        # Build readable summary
        removal_summary = ", ".join(
            f"{prop}={count}" for prop, count in removal_counts.items()
        )

        logger.success(
            f"Cleaned up {label}. n_nodes={total_nodes_touched}. Removals: {removal_summary}"
        )


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


def create_badges_relationships(tx, docs: list, label="User"):
    """
    Create relationships between users/clubs and the badges they earn.
    """
    if label not in ["User", "Club"]:
        raise ValueError("Label must be 'User' or 'Club'.")

    source_label = label
    target_label = "UserBadge" if label == "User" else "ClubBadge"

    rows = []

    for doc in docs:
        label_id = doc.get("_id")
        badges = doc.get("badges") or []
        timestamps = doc.get("badge_timestamps") or []

        for badge, earned_on in zip(badges, timestamps):
            rows.append({
                "label_id": label_id,
                "badge": badge,
                "earned_on": earned_on,
            })

    query = f"""
    UNWIND $rows AS row
    MATCH (a:{source_label} {{_id: row.label_id}})
    MATCH (b:{target_label} {{name: row.badge}})
    MERGE (a)-[rel:HAS_BADGE]->(b)
    SET rel.earnedOn = row.earned_on
    """

    tx.run(query, rows=rows)
    logger.info(f"Created or updated {len(rows)} {source_label}-Badge relationships.")


def user_reads_relationships(tx, user_reads):
    """
    Create relationships between users and the books they read,
    using aggregated stats from agg_user_reads().
    """

    agg_ur = agg_user_reads(user_reads)

    rel_map = {
        "DNF": "DID_NOT_FINISH",
        "Read": "HAS_READ",
        "Paused": "HAS_PAUSED",
        "Reading": "IS_READING",
        "To Read": "WANTS_TO_READ"
    }

    rows = []
    for doc in agg_ur:
        rstatus = doc.get("most_recent_rstatus")
        rel_type = rel_map.get(rstatus)
        if not rel_type:
            continue

        rows.append({
            "user_id": doc.get("user_id"),
            "version_id": doc.get("version_id"),
            "rel_type": rel_type,

            # Optional aggregated properties
            "most_recent_start": doc.get("most_recent_start"),
            "most_recent_read": doc.get("most_recent_read"),
            "most_recent_review": doc.get("most_recent_review"),
            "read_count": doc.get("read_count"),
            "avg_rating": doc.get("avg_rating"),
            "avg_days_to_read": doc.get("avg_days_to_read"),
            "avg_read_rate": doc.get("avg_read_rate"),
            "review": doc.get("notes")
        })

    query = """
    UNWIND $rows AS row
    MATCH (u:User {_id: row.user_id})
    MATCH (b:BookVersion {_id: row.version_id})
    CALL apoc.merge.relationship(u, row.rel_type, {}, {}, b) YIELD rel
    SET
        rel.mostRecentStart = row.most_recent_start,
        rel.mostRecentRead  = row.most_recent_read,
        rel.mostRecentReview = row.most_recent_review,
        rel.readCount       = row.read_count,
        rel.avgRating       = row.avg_rating,
        rel.avgDaysToRead   = row.avg_days_to_read,
        rel.avgReadRate     = row.avg_read_rate
    """

    tx.run(query, rows=rows)
    logger.info(f"Created or updated {len(rows)} User-BookVersion relationships.")


def book_awards_relationships(tx, award_rows):
    """
    Create HAS_AWARD relationships between Book and Award labels.
    Each row must contain:
      - book_id
      - award_id
      - award_name
      - award_category ("" allowed)
      - award_year
      - award_status
    """
    rows = []

    for row in award_rows:
        rows.append({
            "book_id": row["book_id"],
            "award_id": row["award_id"],
            "award_name": row["award_name"],
            "award_category": row.get("award_category", "") or "",
            "award_year": row.get("award_year"),
            "award_status": row.get("award_status", "")
        })

    query = """
    UNWIND $rows AS row
    MATCH (b:Book {_id: row.book_id})
    MATCH (a:Award {_id: row.award_id})
    MERGE (b)-[rel:HAS_AWARD]->(a)
    SET rel.status = row.award_status,
        rel.year = row.award_year
        // Only set category if non-empty
        FOREACH (_ IN CASE WHEN row.award_category <> "" THEN [1] ELSE [] END |
            SET rel.category = row.award_category
        )
    """

    tx.run(query, rows=rows)
    logger.info(f"Created or updated {len(rows)} Book-Award relationships.")


def club_book_relationships(tx, db):
    """Create SELECTED_FOR_PERIOD relationships between Club and Book"""

    cpb = fetch_club_period_books(db)
    rows = []

    for row in cpb:
        if row["selection_status"] != "selected":
            continue
        rows.append({
        "club_id": row["club_id"],
        "book_id": row["book_id"],
        "period": row["period_name"],
        "startdate": row["period_startdate"],
        "enddate": row["period_enddate"],
        "selection_method": row["selection_method"]
        })

    query = """
    UNWIND $rows AS row
    MATCH (c:Club {_id: row.club_id})
    MATCH (b:Book {_id: row.book_id})
    MERGE (c)-[rel:SELECTED_FOR_PERIOD]->(b)
    SET rel.period = row.period,
        rel.startDate = row.startdate,
        rel.endDate = row.enddate,
        rel.selectionMethod = row.selection_method
    """

    tx.run(query, rows=rows)
    logger.info(f"Created or updated {len(rows)} Club-Book relationships.")
