import singer
from singer.catalog import Catalog, CatalogEntry

from .schema import get_stream_schema

LOGGER = singer.get_logger()

def discover(client, config):

    url = client.get_metadata_url()
    metadata = client.get_request(url)

    streams = []

    for table in metadata["tables"]:
        schema = get_stream_schema(table)
        streams.append(CatalogEntry(metadata = {"selected": config.get("selected_by_default")}, **schema))

    return Catalog(streams)