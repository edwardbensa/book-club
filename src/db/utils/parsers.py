"""Parsing utility functions"""

# Import modules
from datetime import datetime
from loguru import logger


# PARSING AND CLEANING FUNCTIONS

def clean_document(doc: dict):
    """
    Removes keys with None, empty lists, or empty strings from a document.
    """
    clean_doc = {k: v for k, v in doc.items() if v is not None and v != [] and v != ''}

    old_keys = list(doc.keys())
    new_keys = list(clean_doc.keys())
    removed_keys = [k for k in old_keys if k not in new_keys]

    return clean_doc, removed_keys


def to_datetime(date_string):
    """
    Converts a date string to a datetime object.
    Returns None if the input is None, empty, or invalid.
    """
    if not date_string or not isinstance(date_string, str):
        return None

    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d'
    ]

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


def to_float(value):
    """
    Converts a value to a float. Returns None if the input is None or an empty string.
    """
    if value is None or value == '':
        return None
    try:
        return float(value)
    except ValueError as e:
        logger.error(f"Invalid float value '{value}': {e}")
        return None


def to_array(field_string):
    """
    Converts a comma-separated string into a list of trimmed strings.
    """
    if not field_string:
        return []
    return [item.strip() for item in field_string.split(',') if item.strip()]


def make_subdocuments(string: str, field_key: str, registry, separator = ';'):
    """
    Parses a separator-separated string into a list of subdocuments
    using the pattern and transform function defined in the subdoc_registry.
    """
    if not string:
        return []

    if string == '':
        return []

    config = registry.get(field_key)
    if not config or not callable(config.get('transform')):
        logger.error(f"Invalid subdocument config for field '{field_key}'")
        return []

    pattern = config.get('pattern')
    transform = config['transform']

    entries = [entry.strip() for entry in string.split(separator) if entry.strip()]

    if pattern:
        transformed_list = []
        for entry in entries:
            match = pattern.match(entry)
            if match:
                transformed_list.append(transform(match))
            else:
                logger.warning(f"No match for entry: '{entry}' in field '{field_key}'")
        return transformed_list

    return [transform(entry) for entry in entries]


def make_array(string: str, field_key: str, registry, separator=';'):
    """
    Parses a separator-separated string into a list of transformed values
    using the transform function defined in the registry.
    """
    if not string:
        return []

    config = registry.get(field_key)
    if not config or not callable(config.get('transform')):
        logger.error(f"Invalid array config for field '{field_key}'")
        return []

    transform = config['transform']
    entries = [entry.strip() for entry in string.split(separator) if entry.strip()]
    return [transform(entry) for entry in entries]
