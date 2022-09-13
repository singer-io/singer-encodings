import codecs
import csv
import sys
from singer_encodings.csv_helper import (CSVHelper, SDC_EXTRA_COLUMN)
import singer
from singer_encodings.jsonl import get_JSONL_iterators
from . import compression

SKIP_FILES_COUNT = 0

LOGGER = singer.get_logger()

def maximize_csv_field_width():
    """Set the max filed size as per the system's maxsize"""

    current_field_size_limit = csv.field_size_limit()
    field_size_limit = sys.maxsize

    if current_field_size_limit != field_size_limit:
        csv.field_size_limit(field_size_limit)
        LOGGER.info("Changed the CSV field size limit from %s to %s", current_field_size_limit, field_size_limit)

def get_row_iterators(iterable, options={}, infer_compression=False, headers_in_catalog=None, with_duplicate_headers=False):
    """Accepts an interable, options and a flag to infer compression and yields
    csv.DictReader objects which can be used to yield CSV rows."""
    global SKIP_FILES_COUNT
    if infer_compression:
        compressed_iterables = compression.infer(iterable, options.get('file_name'))

    for item in compressed_iterables:
        file_name = options.get('file_name')
        file_name_splitted = file_name.split('.')
        extension = file_name_splitted[-1].lower()
        # Get the extension of the zipped file
        if extension == 'zip':
            extension = item.name.split('.')[-1].lower()
            file_name += '/' + item.name
            if extension == 'gz':
                options_copy = dict(options)
                options_copy['file_name'] = options.get('file_name') + '/' + item.name
                yield from get_row_iterators(item, options_copy, infer_compression, headers_in_catalog, with_duplicate_headers)
                continue
        # Get the extension of the gzipped file ie. file.csv.gz -> csv
        elif extension == 'gz':
            # Get file name
            gzip_file_name = item[1]
            # Set iterator 'item'
            item = item[0]
            # Update file name
            file_name += '/' + gzip_file_name if gzip_file_name else ''
            # Get file extension
            extension = gzip_file_name.split('.')[-1].lower() if gzip_file_name else gzip_file_name

        # For GZ files, if the file is gzipped with --no-name, then
        # the 'extension' will be 'None'. Hence, send an empty list
        if not extension or (extension in ['gz', 'zip']):
            SKIP_FILES_COUNT += 1
            LOGGER.warning('Skipping "%s" file as it contains nested compression.', options.get('file_name'))
            yield (file_name, [])
        # If the extension is JSONL then use 'get_JSONL_iterators'
        elif extension == 'jsonl':
            yield (file_name, get_JSONL_iterators(item, options))
        # Assuming the extension is 'csv' of 'txt', then use singer_encoding's 'get_row_iterator'
        else:
            # Maximize the CSV field width
            maximize_csv_field_width()
            yield (file_name, get_row_iterator(item, options=options, headers_in_catalog=headers_in_catalog, with_duplicate_headers=with_duplicate_headers))

def get_row_iterator(iterable, options=None, headers_in_catalog = None, with_duplicate_headers = False):
    """Accepts an interable, options and returns a csv.DictReader or csv.Reader object
    which can be used to yield CSV rows.
    When with_duplicate_headers == true, it will return csv.Reader object
    When with_duplicate_headers == false, it will return csv.DictReader object (default)
    """

    options = options or {}
    reader = []
    headers = set()
    file_stream = codecs.iterdecode(iterable, encoding='utf-8-sig')
    delimiter = options.get('delimiter', ',')

    # Return the CSV key-values along with considering the duplicate headers, if any, in the CSV file
    if with_duplicate_headers:
        # CSV Helper is used to handle duplicate headers.
        # It will store the duplicate headers and its value in the '_sdc_extra' field
        csv_helper = CSVHelper()
        reader = csv_helper.get_row_iterator(file_stream, delimiter, headers_in_catalog)
        headers = set(csv_helper.unique_headers)
    else :
        # Replace any NULL bytes in the line given to the DictReader
        reader = csv.DictReader((line.replace('\0', '') for line in file_stream), fieldnames=None, restkey=SDC_EXTRA_COLUMN, delimiter=delimiter)
        try:
            headers = set(reader.fieldnames)
        except TypeError:
            # handle Nonetype error when empty file is found: tap-SFTP
            pass

    if options.get('key_properties'):
        key_properties = set(options['key_properties'])
        if not key_properties.issubset(headers):
            raise Exception('CSV file missing required headers: {}'
                            .format(key_properties - headers))

    if options.get('date_overrides'):
        date_overrides = set(options['date_overrides'])
        if not date_overrides.issubset(headers):
            raise Exception('CSV file missing date_overrides headers: {}'
                            .format(date_overrides - headers))
    return reader
