import itertools
import json

import singer

LOGGER = singer.get_logger()

def get_JSONL_iterators(iterator, options):
    # Get JSOL rows
    records = get_JSONL_rows(iterator)
    check_jsonl_sample_records, records = itertools.tee(records)

    # Veirfy the 'date_overrides' and 'key_properties' as per the config
    check_key_properties_and_date_overrides_for_jsonl_file(options, check_jsonl_sample_records)
    return records

def check_key_properties_and_date_overrides_for_jsonl_file(options, jsonl_sample_records):
    all_keys = set()
    for record in jsonl_sample_records:
        keys = record.keys()
        all_keys.update(keys)

    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(all_keys):
            raise Exception('JSONL file missing required headers: {}'
                            .format(key_properties - all_keys))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(all_keys):
            raise Exception('JSONL file missing date_overrides headers: {}'
                            .format(date_overrides - all_keys))

def get_JSONL_rows(iterator):
    # Return JSON rows from JSONL file
    for row in iterator:
        decoded_row = row.decode('utf-8')
        if decoded_row.strip():
            row = json.loads(decoded_row)
            # Skip if the row is empty
            if not row:
                continue
        else:
            continue

        yield row