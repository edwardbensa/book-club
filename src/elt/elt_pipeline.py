'''Run full ELT Pipeline.'''

# Import modules
import subprocess
import sys
from loguru import logger

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
        "src/elt/load_data.py",
        "src/elt/transforms/transform_user_reads.py",
        "src/elt/transforms/transform_books.py",
        "src/elt/transforms/transform_creators.py",
        "src/elt/transforms/transform_awards.py",
        "src/elt/transforms/transform_covers.py",
        "src/elt/transforms/cleanup.py"
    ]

    for script in scripts_to_run:
        if not run_script(script):
            logger.error(f"Pipeline stopped due to error in {script}.")
            sys.exit(1) # Exit with an error code

    logger.success("ELT pipeline completed successfully!")

if __name__ == "__main__":
    main()
