import pyarrow as pa
import pyarrow.parquet as pq

def get_row_iterator(file_like_handle):
    pf = pq.ParquetFile(file_like_handle)
    for i in range(pf.num_row_groups):
        rows = pf.read_row_group(i)
        yield from rows.to_pylist()
