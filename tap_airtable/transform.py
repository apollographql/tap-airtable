import re
import singer

LOGGER = singer.get_logger()

def normalize_field_name(name):
    normalized_name = re.sub(r'[^\w\s]', '', name.replace("-", " ").replace("/", " ").lower())
    normalized_name = normalized_name.strip().replace(" ", "_")
    return normalized_name
