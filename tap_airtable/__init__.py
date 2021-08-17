import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_airtable.airtable import Airtable, get_stream_schema, get_tap_data


REQUIRED_CONFIG_KEYS = [
    'token',
    'base_id',
    'selected_by_default'
]

LOGGER = singer.get_logger()

def discover(config):
    
    airtable = Airtable(config.get("base_id"), config.get("token"))
    metadata = airtable.get_metadata().json()

    streams = []

    for table in metadata["tables"]:
        schema = get_stream_schema(table)
        streams.append(CatalogEntry(metadata = {"selected": config.get("selected_by_default")}, **schema))

    return Catalog(streams)

def sync(config, state, catalog):
    
    airtable = Airtable(config.get("base_id"), config.get("token"))

    for stream in catalog.streams:

        if stream.metadata.get("selected"):
            LOGGER.info(f"Syncing stream: {stream.tap_stream_id}")
            
            singer.write_schema(
                stream_name=stream.tap_stream_id,
                schema=stream.schema.to_dict(),
                key_properties=stream.key_properties,
            )
            
            tap_data, offset = get_tap_data(airtable, stream.table)
            singer.write_records(stream.tap_stream_id, tap_data)
            
            while offset:
                tap_data, offset = get_tap_data(airtable, stream.table, offset)
                singer.write_records(stream.tap_stream_id, tap_data)

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(args.config)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config)
        
        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()