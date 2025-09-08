# Import modules
import os
import shutil
from loguru import logger

def empty_directory(directory):
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
