import pyarrow as pa
import pyarrow.parquet as pq

def get_row_iterator(file_like_handle):
    pf = pq.ParquetFile(file_like_handle)
    for i in range(pf.num_row_groups):
        rows = pf.read_row_group(i)
        yield from rows.to_pylist()


def sample_row_iterator(file_like_handle, sample_rate=5, max_records=1000):
    """Row-group-aware sampling iterator, intended ONLY for discovery-time
    schema sampling - do NOT use this for full syncs.

    get_row_iterator() must yield every row, since taps also use it during
    full syncs. That means taps doing discovery-time sampling have had to
    wrap get_row_iterator() and filter rows *after* rows.to_pylist() already
    converted an entire row group's decompressed data into Python objects,
    so sampling provided no memory benefit at all - a file with few/large
    row groups could still cause the same memory spike a full sync would,
    even though discovery only needs a small sample of its rows.

    This function filters at the Arrow level (via Table.take()) *before*
    converting anything to Python objects, so only the rows that will
    actually be sampled are ever materialized as Python objects. It also
    skips decompressing a row group entirely if none of its rows fall on
    the sample_rate boundary, and stops reading further row groups as soon
    as max_records sampled rows have been yielded.

    Args:
        file_like_handle: a file-like object pointing at a Parquet file.
        sample_rate: yield every Nth row, using a row index counted
            globally across all row groups (matching the semantics taps
            previously implemented themselves via `row_idx % sample_rate`).
        max_records: stop once this many rows have been yielded. Pass
            None for no limit (iteration still ends at EOF).
    """
    pf = pq.ParquetFile(file_like_handle)
    current_row = 0
    yielded = 0

    for i in range(pf.num_row_groups):
        num_rows = pf.metadata.row_group(i).num_rows

        # Figure out which rows in this row group we want *before*
        # decompressing anything.
        indices = [
            row_offset for row_offset in range(num_rows)
            if (current_row + row_offset) % sample_rate == 0
        ]
        current_row += num_rows

        if not indices:
            # None of this row group's rows are sampled - skip
            # decompressing it entirely.
            continue

        table = pf.read_row_group(i)
        sampled_table = table.take(indices)

        for row in sampled_table.to_pylist():
            yield row
            yielded += 1
            if max_records is not None and yielded >= max_records:
                return
