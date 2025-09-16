import pyarrow as pa
import pyarrow.parquet as pq
import singertools.infer_schema as infer

def get_row_iterator(file_like_handle):
    pf = pq.ParquetFile(file_like_handle)
    for i in range(pf.num_row_groups):
        rows = pf.read_row_group(i)
        yield from rows.to_pylist()

def get_schema(file_like_handle, props):
    infer.OBSERVED_TYPES = {}
    sample_size = 100
    for row in get_row_iterator(file_like_handle):
        sample_size -= 1
        if sample_size == 0:
            break
        infer.add_observations([], row)

    return {
        'type': 'SCHEMA',
        'stream': props['stream_name'],
        'schema': infer.to_json_schema(infer.OBSERVED_TYPES)
    }
