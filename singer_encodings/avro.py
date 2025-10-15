from fastavro import reader

def get_row_iterator(file_like_handle):
    avro_reader = reader(file_like_handle)
    yield from avro_reader
