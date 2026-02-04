"""Upsert data into AuraDB"""

# Imports
import datetime
from src.db.utils.security import decrypt_field
from src.db.utils.connectors import connect_mongodb, connect_auradb
from src.db.utils.polyglot import (fetch_from_mongo, upsert_nodes, process_books,
                                   create_relationships, create_badges_relationships,
                                   user_reads_relationships, book_awards_relationships,
                                   club_book_relationships, cleanup_nodes, clear_all_nodes
                                  )


# Connect to databases
db, mongo = connect_mongodb()
neo4j_driver = connect_auradb()

# Field maps
books_map = {
    "author": "author.name",
    "author_id": "author._id",
    "series": "series.name",
    "series_id": "series._id"
}

bv_map = {
    "translator": "translator.name",
    "translator_id": "translator._id",
    "illustrator": "illustrator.name",
    "illustrator_id": "illustrator._id",
    "narrator": "narrator.name",
    "narrator_id": "narrator._id",
    "cover_artist": "cover_artist.name",
    "cover_artist_id": "cover_artist._id",
    "contributor": "contributor.name",
    "contributor_id": "contributor._id",
    "publisher_id": "publisher._id",
    "publisher": "publisher.name"
    }

user_map = {
    "club_ids": "clubs._id",
    "badges": "badges.name",
    "badge_timestamps": "badges.timestamp"
    }

club_map = {
    "badges": "badges.name",
    "badge_timestamps": "badges.timestamp"
    }

# Extract from MongoDB
books = fetch_from_mongo(db["books"], field_map=books_map, exclude_fields=["date_added"])
book_versions = fetch_from_mongo(db["book_versions"], field_map=bv_map)
book_series = fetch_from_mongo(db["book_series"], exclude_fields=["date_added"])
genres = fetch_from_mongo(db["genres"], exclude_fields=["date_added"])
awards = fetch_from_mongo(db["awards"], exclude_fields=["date_added"])
creators = fetch_from_mongo(db["creators"], exclude_fields=["date_added"])
creator_roles = fetch_from_mongo(db["creator_roles"])
publishers = fetch_from_mongo(db["publishers"], exclude_fields=["date_added"])
formats = fetch_from_mongo(db["formats"])
languages = fetch_from_mongo(db["languages"])
user_badges = fetch_from_mongo(db["user_badges"], exclude_fields=["date_added"])
club_badges = fetch_from_mongo(db["club_badges"], exclude_fields=["date_added"])
countries = fetch_from_mongo(db["countries"])

excluded_user_fields = [
    "firstname", "lastname", "email_address", "password", "dob", "gender", "city", "state",
    "is_admin", "last_active_date"
    ]
excluded_club_fields = ["member_permissions", "join_requests", "moderators"]

users = fetch_from_mongo(db["users"], field_map=user_map, exclude_fields=excluded_user_fields)
clubs = fetch_from_mongo(db["clubs"], field_map=club_map, exclude_fields=excluded_club_fields)
user_reads = fetch_from_mongo(db["user_reads"])

# Add information
current_year = datetime.date.today().year
for user in users:
    goals = user["reading_goal"]
    user["reading_goal"] = next((g["goal"] for g in goals if g["year"] == current_year), "N/A")
    country = decrypt_field(user["country"], user["key_version"])
    user["country"] = country
    user.pop("key_version", None)

for creator in creators:
    lastname = creator.get("lastname", None)
    creator["name"] = creator["firstname"] + f" {lastname}" if lastname else ""

books, book_awards = process_books(books)


# Upsert nodes to Neo4j
clear_all_nodes(neo4j_driver)
with neo4j_driver.session() as session:
    session.execute_write(upsert_nodes, "Book", books)
    session.execute_write(upsert_nodes, "BookVersion", book_versions)
    session.execute_write(upsert_nodes, "BookSeries", book_series)
    session.execute_write(upsert_nodes, "Genre", genres)
    session.execute_write(upsert_nodes, "Award", awards)
    session.execute_write(upsert_nodes, "Creator", creators)
    session.execute_write(upsert_nodes, "CreatorRole", creator_roles)
    session.execute_write(upsert_nodes, "Publisher", publishers)
    session.execute_write(upsert_nodes, "Format", formats)
    session.execute_write(upsert_nodes, "Language", languages)
    session.execute_write(upsert_nodes, "User", users)
    session.execute_write(upsert_nodes, "Club", clubs)
    session.execute_write(upsert_nodes, "UserBadge", user_badges)
    session.execute_write(upsert_nodes, "ClubBadge", user_badges)
    session.execute_write(upsert_nodes, "Country", countries)

# Edge maps
book_genre_map = {"labels": ["Book", "Genre"], "props": ["genre", "name"]}
bv_book_map = {"labels": ["BookVersion", "Book"], "props": ["book_id", "_id"]}
book_series_map = {"labels": ["Book", "BookSeries"], "props": ["series_id", "_id"]}
book_author_map = {"labels": ["Book", "Creator"], "props": ["author_id", "_id"]}
bv_narrator_map = {"labels": ["BookVersion", "Creator"], "props": ["narrator_id", "_id"]}
bv_cartist_map = {"labels": ["BookVersion", "Creator"], "props": ["cover_artist_id", "_id"]}
bv_illustrator_map = {"labels": ["BookVersion", "Creator"], "props": ["illustrator_id", "_id"]}
bv_translator_map = {"labels": ["BookVersion", "Creator"], "props": ["translator_id", "_id"]}
bv_publisher_map = {"labels": ["BookVersion", "Publisher"], "props": ["publisher_id", "_id"]}
bv_language_map = {"labels": ["BookVersion", "Language"], "props": ["language", "name"]}
bv_format_map = {"labels": ["BookVersion", "Format"], "props": ["format", "name"]}
creator_cr_map = {"labels": ["Creator", "CreatorRole"], "props": ["roles", "name"]}
user_club_map = {"labels": ["User", "Club"], "props": ["club_ids", "_id"]}
user_country_map = {"labels": ["User", "Country"], "props": ["country", "name"]}
user_genre_map1 = {"labels": ["User", "Genre"], "props": ["preferred_genres", "name"]}
user_genre_map2 = {"labels": ["User", "Genre"], "props": ["forbidden_genres", "name"]}
club_genre_map = {"labels": ["Club", "Genre"], "props": ["preferred_genres", "name"]}

# Create node relationships
with neo4j_driver.session() as session:
    session.execute_write(create_relationships, book_genre_map, "HAS_GENRE")
    session.execute_write(create_relationships, bv_book_map, "VERSION_OF")
    session.execute_write(create_relationships, book_series_map, "ENTRY_IN")
    session.execute_write(create_relationships, book_author_map, "AUTHORED_BY")
    session.execute_write(create_relationships, bv_narrator_map, "NARRATED_BY")
    session.execute_write(create_relationships, bv_cartist_map, "COVER_ART_BY")
    session.execute_write(create_relationships, bv_illustrator_map, "ILLUSTRATION_BY")
    session.execute_write(create_relationships, bv_translator_map, "TRANSLATED_BY")
    session.execute_write(create_relationships, bv_publisher_map, "PUBLISHED_BY")
    session.execute_write(create_relationships, bv_language_map, "HAS_LANGUAGE")
    session.execute_write(create_relationships, bv_format_map, "HAS_FORMAT")
    session.execute_write(create_relationships, creator_cr_map, "HAS_ROLE")
    session.execute_write(create_relationships, user_club_map, "MEMBER_OF")
    session.execute_write(create_relationships, user_country_map, "LIVES_IN")
    session.execute_write(create_relationships, user_genre_map1, "PREFERS_GENRE")
    session.execute_write(create_relationships, user_genre_map2, "AVOIDS_GENRE")
    session.execute_write(create_relationships, club_genre_map, "PREFERS_GENRE")

    session.execute_write(user_reads_relationships, user_reads)
    session.execute_write(create_badges_relationships, users, "User")
    session.execute_write(create_badges_relationships, clubs, "Club")
    session.execute_write(book_awards_relationships, book_awards)
    session.execute_write(club_book_relationships, db)

# Cleanup
cleanup_dict = {
    "Book": ["author_id", "series_id"],
    "BookVersion": ["book_id", "publisher_id", "narrator_id",
                    "illustrator_id", "translator_id", "cover_artist_id"],
    "User": ["club_ids", "badge_timestamps"],
    "Club": ["created_by"],
}
cleanup_nodes(neo4j_driver, cleanup_dict)

neo4j_driver.close()
