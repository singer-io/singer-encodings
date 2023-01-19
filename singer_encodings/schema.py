import singer
LOGGER = singer.get_logger()

def generate_schema(samples, table_spec):
    """Function to generate the schema as the records"""
    counts = {}

    # Loop over the samples and create the count of the datatypes
    # {'id': {'integer': 45}, 'name': {'string' : 45}}
    for sample in samples:
        counts = count_sample(sample, counts, table_spec)

    # Loop over the counts and generate the schema based on the datatype
    for key, value in counts.items():
        # Get the datatype
        datatype = pick_datatype(value)

        # Handle 'list' datatype schema separately
        if 'list.' in datatype:
            child_datatype = datatype.rsplit('.', maxsplit=1)[-1]
            counts[key] = {
                'anyOf': [
                    {'type': 'array', 'items': datatype_schema(child_datatype)},
                    {'type': ['null', 'string']}
                ]
            }
        elif datatype == 'list':
            counts[key] = {
                'anyOf': [
                    {'type': 'array', 'items': {'type': ['null', 'string']}},
                    {'type': ['null', 'string']}
                ]
            }
        else:
            counts[key] = datatype_schema(datatype)

    return counts

def datatype_schema(datatype):
    """Function to create schema for the field as per the datatype"""
    if datatype == 'date-time':
        schema = {
            'anyOf': [
                {'type': ['null', 'string'], 'format': 'date-time'},
                {'type': ['null', 'string']}
            ]
        }
    elif datatype == 'dict':
        schema = {
            'anyOf': [
                {'type': 'object', 'properties': {}},
                {'type': ['null', 'string']}
            ]
        }
    else:
        types = ['null', datatype]
        # Add 'string' as fallback condition
        if datatype != 'string':
            types.append('string')
        schema = {
            'type': types,
        }
    return schema

def pick_datatype(counts):
    """Function to get the datatype from the counts"""
    # Default return
    to_return = 'string'
    list_of_datatypes = ['list.date-time', 'list.dict', 'list.number',
                         'list.integer', 'list.string', 'list', 'date-time', 'dict']

    # Look if the datatype is 'list' by looping over the 'list_of_datatypes'
    for data_types in list_of_datatypes:
        if counts.get(data_types, 0) > 0:
            return data_types

    # Return the integer or number datatype
    if len(counts) == 1:
        if counts.get('integer', 0) > 0:
            to_return = 'integer'
        elif counts.get('number', 0) > 0:
            to_return = 'number'

    # If the data is of integer and number, then return number as the datatype
    elif(len(counts) == 2 and
        counts.get('integer', 0) > 0 and
        counts.get('number', 0) > 0):
        to_return = 'number'

    return to_return

def count_sample(sample, counts, table_spec):
    """Function to count the records as per the datatype"""
    for key, value in sample.items():
        if key not in counts:
            counts[key] = {}

        date_overrides = table_spec.get('date_overrides', [])
        # Get the datatype
        datatype = infer(key, value, date_overrides)

        # Increment the count of the datatype
        if datatype is not None:
            counts[key][datatype] = counts[key].get(datatype, 0) + 1

    return counts

def infer(key, datum, date_overrides, second_call=False):
    """Function to return the inferred data type"""
    if datum is None or datum == '':
        return None

    try:
        if isinstance(datum, list):
            # Default the datatype as list of strings
            data_type = 'string'
            if second_call: # Use string for nested list
                LOGGER.warning(
                    'Unsupported type for "%s", List inside list is not supported hence will be treated as a string', key)
            # Empty list
            elif not datum:
                data_type = 'list'
            # Get the datatype of list elements
            else:
                data_type = 'list.' + infer(key, datum[0], date_overrides, second_call=True)
            return data_type

        # Send 'date-time' for date_overrides fields
        if key in date_overrides:
            return 'date-time'

        if isinstance(datum, dict):
            return 'dict'

        try:
            # Convert the data into the string before integer conversion
            # As for CSV, all the data will be replicated into the string as a result, int("1.1") will result into ValueError
            # Whereas for JSONL, all the data will be replicated into original form thus, int(1.1) will not raise any error.
            # Hence, wrong datatype will be assigned
            int(str(datum))
            return 'integer'
        except (ValueError, TypeError):
            pass

        try:
            # numbers are NOT floats, they are DECIMALS
            float(str(datum))
            return 'number'
        except (ValueError, TypeError):
            pass

    except (ValueError, TypeError):
        pass

    return 'string'
