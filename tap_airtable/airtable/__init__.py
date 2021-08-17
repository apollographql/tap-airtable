import requests
import re
import os
import json
import singer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

LOGGER = singer.get_logger()

STRING_TYPES = set([
    'lookup',
    'singleLineText',
    'singleSelect',
    'phoneNumber',
    'email',
    'url',
    'multilineText',
    'rollup',
    'rating',
    'duration',
    'richText'
])

NUMBER_TYPES = set([
    'number',
    'autoNumber',
    'count',
    'legacyPercentTimes100'
])

DATE_TYPES = set([
    'dateTime',
    'date',
    'createdTime'
])

ARRAY_TYPES = set([
    'multipleRecordLinks',
    'multipleSelects',
    'multipleAttachments',
    'multipleCollaborators'
])


def get_property_schema(field):
    
    property_schema = {}
    airtable_type = field.get("type")

    if airtable_type in STRING_TYPES:
        property_schema["type"] = ["null", "string"]
    elif airtable_type in DATE_TYPES:
        date_type = {"type": "string", "format": "date-time"}
        string_type = {"type": ["null", "string"]}
        property_schema["anyOf"] = [date_type, string_type]
    elif airtable_type in NUMBER_TYPES:
        property_schema['type'] = ["null", "number"]
    elif airtable_type == "checkbox":
        property_schema['type'] = ["null", "boolean"]
    elif airtable_type in ARRAY_TYPES:
        property_schema['items'] = {"type": "string"}
        property_schema['type'] = ["null", "array"]
    elif airtable_type == "formula":
        property_schema = get_property_schema(field.get("options").get("result"))
    else:
        raise Exception(f"Found unsupported type: {airtable_type}.")

    return property_schema

def get_stream_schema(table):

    stream_schema = {}
    properties = {"id": {"type": "string"}}

    table_name = table.get("name")
    stream_name = table_name.lower().replace(" ", "_")
    id_field = table.get("primaryFieldId")

    for field in table.get("fields"):
        property_name = normalize_field_name(field.get("name"))
        property_schema = get_property_schema(field.get("config"))
        properties[property_name] = property_schema
    
    stream_schema["table"] = table_name
    stream_schema["tap_stream_id"] = stream_name
    stream_schema["stream"] = stream_name
    stream_schema["schema"] = Schema.from_dict({"type": ["null", "object"], "properties": properties})
    stream_schema["key_properties"] = ["id"]

    return stream_schema

def normalize_field_name(name):
    
    normalized_name = re.sub(r'[^\w\s]', '', name.replace("-", " ").replace("/", " ").lower()).replace(" ", "_")
    return normalized_name

def get_tap_data(airtable, table, offset = None):
    
    response = airtable.get_response(table, offset).json()
    tap_data = response.get('records')
    offset = response.get("offset")

    normalized_rows = []

    for row in tap_data:
        normalized_row = {"id": row.get("id")}
        
        for k, v in row.get("fields").items():
            normalized_row[normalize_field_name(k)] = v
        
        normalized_rows.append(normalized_row)
    
    return normalized_rows, offset

class Airtable():
    def __init__(self, base_id, token):
        self.base_id = base_id
        self.headers = {"Authorization": f"Bearer {token}"}

    def get_response(self, table, offset=None):
        
        table = table.replace('/', '%2F')
        url = f"https://api.airtable.com/v0/{self.base_id}/{table}"

        if offset:
            url = f"{url}?offset={offset}"
        
        response = requests.get(url, headers = self.headers)
        
        return response

    def get_metadata(self):
        
        url = f"https://api.airtable.com/v2/meta/{self.base_id}"
        response = requests.get(url, headers = self.headers)
        
        return response
