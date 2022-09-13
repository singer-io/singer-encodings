from json import JSONDecodeError
import singer
from singer_encodings.schema import generate_schema
from singer_encodings.csv import SDC_EXTRA_COLUMN, get_row_iterators, SKIP_FILES_COUNT

SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"

# TODO: Add additional logging
LOGGER = singer.get_logger()

def get_sdc_columns():
    return {
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {
            'type': 'array',
            'items': {
                'anyOf': [
                    {'type': 'object', 'properties': {}},
                    {'type': 'string'}
                ]
            }
        }
    }

# TODO: conn needs get_files and get_file_handle functions
def get_schema_for_table(conn, table_spec, sample_rate=1):
    files = conn.get_files(table_spec['search_prefix'], table_spec['search_pattern'])

    if not files:
        return {}

    samples = sample_files(conn, table_spec, files, sample_rate=sample_rate)

    schema = generate_schema(samples, table_spec)

    if SKIP_FILES_COUNT:
        LOGGER.warning("%s files got skipped during the last sampling.", SKIP_FILES_COUNT)

    # return empty if there is no schema generated
    if not schema:
        return {
            'type': 'object',
            'properties': {},
        }

    data_schema = {
        **schema,
        **get_sdc_columns()
    }

    return {
        'type': 'object',
        'properties': data_schema,
    }

def sample_file(conn, table_spec, f, sample_rate, max_records):
    table_name = table_spec['table_name']
    plurality = "s" if sample_rate != 1 else ""

    global SKIP_FILES_COUNT

    LOGGER.info('Sampling %s (max records: %s, sample rate: %s)',
                f['filepath'],
                max_records,
                sample_rate)

    samples = []
    try:
        file_handle = conn.get_file_handle(f)
    except OSError:
        return (False, samples)

    # Add file_name to opts and flag infer_compression to support gzipped files
    opts = {'key_properties': table_spec['key_properties'],
            'delimiter': table_spec.get('delimiter', ','),
            'file_name': f['filepath']}

    readers = get_row_iterators(file_handle, options=opts, infer_compression=True, with_duplicate_headers=True)

    for _, reader in readers:
        current_row = 0
        for row in reader:
            if (current_row % sample_rate) == 0:
                if row.get(SDC_EXTRA_COLUMN):
                    row.pop(SDC_EXTRA_COLUMN)
                samples.append(row)

            current_row += 1

            if len(samples) >= max_records:
                break

    LOGGER.info("Sampled %s rows from %s", len(samples), f['filepath'])
    # Empty sample to show field selection, if needed
    empty_file = False
    if len(samples) == 0:
        empty_file = True
        SKIP_FILES_COUNT += 1

    return (empty_file, samples)

# pylint: disable=too-many-arguments
def sample_files(conn, table_spec, files,
                 sample_rate=1, max_records=1000, max_files=5):
    LOGGER.info("Sampling files (max files: %s)", max_files)
    to_return = []
    empty_samples = []

    files_so_far = 0

    global SKIP_FILES_COUNT

    for f in files:
        try:
            empty_file, samples = sample_file(conn, table_spec, f, sample_rate, max_records)
        except (UnicodeDecodeError, JSONDecodeError):
            LOGGER.warning('Skipping %s file as parsing failed. Verify an extension of the file.', f['filepath'])
            SKIP_FILES_COUNT += 1
            continue

        if empty_file:
            empty_samples += samples
        else:
            to_return += samples

        files_so_far += 1

        if files_so_far >= max_files:
            break

    if not any(to_return):
        return empty_samples

    return to_return

def infer(datum):
    """
    Returns the inferred data type
    """
    if datum is None or datum == '':
        return None

    try:
        int(datum)
        return 'integer'
    except (ValueError, TypeError):
        pass

    try:
        #numbers are NOT floats, they are DECIMALS
        float(datum)
        return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'

def count_sample(sample, counts, table_spec):
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        date_overrides = table_spec.get('date_overrides', [])
        if key in date_overrides:
            datatype = "date-time"
        else:
            datatype = infer(value)

        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts

def pick_datatype(counts):
    """
    If the underlying records are ONLY of type `integer`, `number`,
    or `date-time`, then return that datatype.

    If the underlying records are of type `integer` and `number` only,
    return `number`.

    Otherwise return `string`.
    """
    to_return = 'string'

    if counts.get('date-time', 0) > 0:
        return 'date-time'

    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    elif(len(counts) == 2 and
         counts.get('integer', 0) > 0 and
         counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return
