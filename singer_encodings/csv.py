import codecs
import csv

SDC_EXTRA_COLUMN = "_sdc_extra"

def generator_wrapper(reader):
    for row in reader:
        to_return = {}
        for key, value in row.items():
            if key is None:
                key = SDC_EXTRA_COLUMN

            formatted_key = key

            to_return[formatted_key] = value

        yield to_return


def get_row_iterator(iterable, options=None):
    options = options or {}
    file_stream = codecs.iterdecode(iterable, encoding='utf-8')

    field_names = None

    # Replace any NULL bytes in the line given to the DictReader
    reader = csv.DictReader((line.replace('\0', '') for line in file_stream), fieldnames=field_names)

    headers = set(reader.fieldnames)
    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('CSV file missing required headers: {}, file only contains headers for fields: {}'
                            .format(key_properties - headers, headers))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('CSV file missing date_overrides headers: {}, file only contains headers for fields: {}'
                            .format(date_overrides - headers, headers))

    return generator_wrapper(reader)
