'''Run full ELT Pipeline.'''

# Import modules
import subprocess
import sys
from loguru import logger
from src.db.utils.files import wipe_directory
from src.config import TRANSFORMED_COLLECTIONS_DIR

def run_script(script_path):
    """
    Runs a Python script using a subprocess and logs its output.
    Returns True if the script ran successfully, False otherwise.
    """
    logger.info(f"Starting execution of {script_path}...")
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Successfully ran {script_path}")
        logger.info("Script output:")
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run {script_path}. Script returned an error.")
        logger.error(f"Stdout:\n{e.stdout}")
        logger.error(f"Stderr:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logger.error(f"The script file was not found at {script_path}. Please check the path.")
        return False
    except (KeyError, TypeError, ValueError) as e:
        logger.error(f"An unexpected error occurred while running {script_path}: {e}")
        return False

def main():
    """
    Main function to run the ELT pipeline in sequence.
    """
    scripts_to_run = [
        "src/db/etl/extract/sync_gsheet.py",
        "src/db/etl/transforms/transform_clubs.py",
        "src/db/etl/transforms/transform_users.py",
        "src/db/etl/transforms/transform_books.py",
        "src/db/etl/transforms/transform_creators.py",
        "src/db/etl/transforms/transform_awards.py",
        "src/db/etl/utilityscripts/sync_images.py",
        "src/db/etl/transforms/cleanup.py",
        "src/db/etl/load/load_mongo.py",
        "src/db/etl/load/load_aura.py"
    ]

    for script in scripts_to_run:
        if not run_script(script):
            logger.error(f"Pipeline stopped due to error in {script}.")
            sys.exit(1) # Exit with an error code

    logger.success("ELT pipeline completed successfully!")

if __name__ == "__main__":
    wipe_directory(TRANSFORMED_COLLECTIONS_DIR)
    main()
