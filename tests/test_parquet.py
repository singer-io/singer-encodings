import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from unittest import mock
from singer_encodings.parquet import get_row_iterator, sample_row_iterator, is_empty


def make_parquet_file(num_rows=100, row_group_size=None, compression='snappy'):
    parquet_file = tempfile.TemporaryFile('w+b')
    data = {
        "id": list(range(1, num_rows + 1)),
        "name": [f"user_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 1.5 for i in range(1, num_rows + 1)],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file, row_group_size=row_group_size, compression=compression)
    parquet_file.seek(0)
    return parquet_file


class TestGetRowIterator(unittest.TestCase):
    def setUp(self):
        self.parquet_file = make_parquet_file(num_rows=100)

    def tearDown(self):
        self.parquet_file.close()

    def test(self):
        row_iterator = get_row_iterator(self.parquet_file)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})
        self.assertEqual(rows[99], {'id': 100, 'name': 'user_100', 'value': 150})
        self.assertEqual(len(rows), 100)


class TestSampleRowIterator(unittest.TestCase):
    def tearDown(self):
        self.parquet_file.close()

    def test_matches_post_hoc_sampling_of_get_row_iterator(self):
        # sample_row_iterator should yield exactly the rows that the old
        # "wrap get_row_iterator() and filter after the fact" approach
        # would have produced, for the same sample_rate/max_records.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        sample_rate = 7
        max_records = 1000

        expected = []
        for idx, row in enumerate(get_row_iterator(self.parquet_file)):
            if idx % sample_rate == 0:
                expected.append(row)
                if len(expected) >= max_records:
                    break

        self.parquet_file.seek(0)
        actual = list(sample_row_iterator(self.parquet_file, sample_rate, max_records))

        self.assertEqual(actual, expected)

    def test_respects_max_records_across_row_groups(self):
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        rows = list(sample_row_iterator(self.parquet_file, sample_rate=1, max_records=25))

        self.assertEqual(len(rows), 25)
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})
        self.assertEqual(rows[-1], {'id': 25, 'name': 'user_25', 'value': 37.5})

    def test_default_sample_rate_and_max_records(self):
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        rows = list(sample_row_iterator(self.parquet_file))

        # default sample_rate=5 -> ids 1, 6, 11, ... ; default max_records=1000
        # is well above the 20 rows sample_rate=5 would produce from 100 rows.
        self.assertEqual(len(rows), 20)
        self.assertEqual([r['id'] for r in rows], list(range(1, 101, 5)))

    def test_max_records_none_means_unlimited(self):
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        rows = list(sample_row_iterator(self.parquet_file, sample_rate=10, max_records=None))

        self.assertEqual(len(rows), 10)
        self.assertEqual([r['id'] for r in rows], list(range(1, 101, 10)))

    def test_stops_reading_row_groups_once_max_records_reached(self):
        # 10 row groups of 10 rows each; sample_rate=1 means every row in
        # row group 0 (rows 0-9) already satisfies max_records=5, so
        # read_row_group should only ever be called once.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)
        original_read_row_group = pq.ParquetFile.read_row_group

        with mock.patch.object(pq.ParquetFile, 'read_row_group', autospec=True) as mocked_read:
            mocked_read.side_effect = original_read_row_group
            rows = list(sample_row_iterator(self.parquet_file, sample_rate=1, max_records=5))

        self.assertEqual(len(rows), 5)
        self.assertEqual(mocked_read.call_count, 1)

    def test_skips_decompressing_row_groups_with_no_sampled_rows(self):
        # 10 row groups of 10 rows each; sample_rate=25 only lands on rows
        # 0, 25, 50, 75 - i.e. row groups 0, 2, 5, 7. The other 6 row
        # groups should never be decompressed via read_row_group at all.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)
        original_read_row_group = pq.ParquetFile.read_row_group

        with mock.patch.object(pq.ParquetFile, 'read_row_group', autospec=True) as mocked_read:
            mocked_read.side_effect = original_read_row_group
            rows = list(sample_row_iterator(self.parquet_file, sample_rate=25, max_records=1000))

        self.assertEqual([r['id'] for r in rows], [1, 26, 51, 76])
        self.assertEqual(mocked_read.call_count, 4)

    def test_caps_materialized_rows_to_max_records_within_a_single_large_row_group(self):
        # A single row group large enough that sample_rate alone selects far
        # more rows than max_records. Without capping `indices` to the
        # remaining budget before take()/to_pylist(), the whole
        # sample_rate-filtered subset of the row group gets converted to
        # Python objects up front, regardless of max_records - the exact
        # single-large-row-group scenario this ticket is about.
        self.parquet_file = make_parquet_file(num_rows=1000, row_group_size=1000)
        original_read_row_group = pq.ParquetFile.read_row_group
        take_call_sizes = []

        class TakeSpy:
            # Wraps the real pa.Table so we can observe how many indices
            # take() is called with, without needing to patch pyarrow's
            # immutable Table type directly.
            def __init__(self, table):
                self._table = table

            def take(self, indices):
                take_call_sizes.append(len(indices))
                return self._table.take(indices)

        def spy_read_row_group(self, *args, **kwargs):
            return TakeSpy(original_read_row_group(self, *args, **kwargs))

        with mock.patch.object(pq.ParquetFile, 'read_row_group', autospec=True) as mocked_read:
            mocked_read.side_effect = spy_read_row_group
            # sample_rate=1 -> every one of the 1000 rows in the row group
            # matches the sample boundary, but max_records=10 should mean
            # take() is only ever called with 10 indices, not 1000.
            rows = list(sample_row_iterator(self.parquet_file, sample_rate=1, max_records=10))

        self.assertEqual(len(rows), 10)
        self.assertEqual(take_call_sizes, [10])

    def test_empty_file(self):
        self.parquet_file = make_parquet_file(num_rows=0)

        rows = list(sample_row_iterator(self.parquet_file, sample_rate=5, max_records=1000))

        self.assertEqual(rows, [])


class TestSnappyCompression(unittest.TestCase):
    # Snappy is pyarrow's default Parquet compression codec, so every
    # other test in this file already exercises it implicitly via
    # make_parquet_file(). This test makes that coverage explicit: it
    # writes a file with compression='snappy', confirms the codec is
    # actually SNAPPY at the row-group level, and verifies both
    # get_row_iterator() (full sync) and sample_row_iterator()
    # (schema discovery) decode it correctly.
    def tearDown(self):
        self.parquet_file.close()

    def test_row_group_reports_snappy_codec(self):
        self.parquet_file = make_parquet_file(num_rows=10, compression='snappy')

        pf = pq.ParquetFile(self.parquet_file)
        compression = pf.metadata.row_group(0).column(0).compression

        self.assertEqual(compression, 'SNAPPY')

    def test_get_row_iterator_reads_snappy_compressed_file(self):
        self.parquet_file = make_parquet_file(num_rows=10, compression='snappy')

        rows = list(get_row_iterator(self.parquet_file))

        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})

    def test_sample_row_iterator_reads_snappy_compressed_file(self):
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10, compression='snappy')

        rows = list(sample_row_iterator(self.parquet_file, sample_rate=25, max_records=1000))

        self.assertEqual([r['id'] for r in rows], [1, 26, 51, 76])


class TestIsEmpty(unittest.TestCase):
    def tearDown(self):
        self.parquet_file.close()

    def test_true_for_a_file_with_zero_rows(self):
        self.parquet_file = make_parquet_file(num_rows=0)

        self.assertTrue(is_empty(self.parquet_file))

    def test_false_for_a_file_with_rows(self):
        self.parquet_file = make_parquet_file(num_rows=1)

        self.assertFalse(is_empty(self.parquet_file))

    def test_does_not_decompress_any_row_group(self):
        # is_empty() should only need the footer metadata - never call
        # read_row_group.
        self.parquet_file = make_parquet_file(num_rows=100, row_group_size=10)

        with mock.patch.object(pq.ParquetFile, 'read_row_group') as mocked_read:
            is_empty(self.parquet_file)

        mocked_read.assert_not_called()
