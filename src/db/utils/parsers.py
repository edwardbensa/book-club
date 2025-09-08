# Import modules
from datetime import datetime
from loguru import logger


# PARSING AND CLEANING FUNCTIONS

def clean_document(doc):
    """
    Removes keys with None, empty lists, or empty strings from a document.
    """
    return {k: v for k, v in doc.items() if v is not None and v != [] and v != ''}


def to_datetime(date_string):
    """
    Converts a date string to a datetime object.
    Supports both 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM' formats.
    Returns None if the input is None, empty, or invalid.
    """
    if not date_string:
        return None

    formats = ['%Y-%m-%d %H:%M', '%Y-%m-%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except ValueError:
            continue

    logger.error(f"Invalid date format for '{date_string}'")
    return None


def to_int(value):
    """
    Converts a value to an integer. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return int(value)
    except ValueError as e:
        logger.error(f"Invalid integer value '{value}': {e}")
        return None
