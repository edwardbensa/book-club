"""Sync images with Azure Blob Storage"""

# Import necessary modules
from loguru import logger
from src.db.utils.connectors import connect_azure_blob, sync_images
from src.db.utils.files import download_images, selective_delete
from src.config import COVER_ART_DIR


# Connect to Azure Blob Storage
blob_service_client = connect_azure_blob()
CONTAINER_NAME = 'cover-art'


if __name__ == "__main__":
    download_images("book_versions", "cover_url", "cover", COVER_ART_DIR)
    sync_images(blob_service_client, CONTAINER_NAME, COVER_ART_DIR, 'cover')
    selective_delete(COVER_ART_DIR, "cover")

    logger.info("Images downloaded and synced to Azure containers.")
