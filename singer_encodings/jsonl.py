import itertools
import json

import singer

LOGGER = singer.get_logger()

def get_JSONL_iterators(iterator, options):
    """Function to return data iterator for JSONL files"""
    # Get JSOL rows
    records = get_JSONL_rows(iterator)
    check_jsonl_sample_records, records = itertools.tee(records)

    # Veirfy the 'date_overrides' and 'key_properties' as per the config
    check_key_properties_and_date_overrides_for_jsonl_file(options, check_jsonl_sample_records)
    return records

def check_key_properties_and_date_overrides_for_jsonl_file(options, jsonl_sample_records):
    """
    Function to check the 'key_properties' and 'date_overrides'
    are provided in the JSON Data as expected from the config
    """
    for record in jsonl_sample_records:
        keys = record.keys()

        # Verify if the 'key_properties' field is passed in the data
        if options.get('key_properties'):
            key_properties = set(options['key_properties'])
            if not key_properties.issubset(keys):
                raise Exception('JSONL file "{}" missing required headers: {}'
                                .format(options.get('file_name'), key_properties - keys))

        # Verify if the 'date_overrides' field is passed in the data
        if options.get('date_overrides'):
            date_overrides = set(options['date_overrides'])
            if not date_overrides.issubset(keys):
                raise Exception('JSONL file "{}" missing date_overrides headers: {}'
                                .format(options.get('file_name'), date_overrides - keys))

def get_JSONL_rows(iterator):
    """Return JSON rows from JSONL file"""
    for row in iterator:
        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
        else:
            continue

        yield row
