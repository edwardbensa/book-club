"""Wipe databases and start over"""

# Imports
from loguru import logger
from src.db.utils.connectors import connect_mongodb, connect_auradb
from src.db.utils.polyglot import clear_all_nodes
from src.db.utils.db_ops import drop_all_collections


# Connect to databases
db, client = connect_mongodb()
neo4j_driver = connect_auradb()

def main(wipe: str="all"):
    """Choose which database to wipe"""
    if wipe == "mongo":
        logger.warning("Dropping all collections in MongoDB...")
        drop_all_collections(db)
        logger.success("MongoDB collections successfully dropped...")
    elif wipe == "aura":
        logger.warning("Clearing all nodes and relationships in AuraDB...")
        clear_all_nodes(neo4j_driver)
        logger.success("AuraDB nodes and relationships successfully cleared...")
    else:
        logger.warning("Clearing all data from MongoDB and AuraDB...")
        drop_all_collections(db)
        clear_all_nodes(neo4j_driver)
        logger.success("MongoDB and AuraDB nodes successfully wiped...")

# Run
if __name__ == "__main__":
    main()
