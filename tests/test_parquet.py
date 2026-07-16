import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from unittest import mock
from singer_encodings.parquet import get_row_iterator, sample_row_iterator


def make_parquet_file(num_rows=100, row_group_size=None):
    parquet_file = tempfile.TemporaryFile('w+b')
    data = {
        "id": list(range(1, num_rows + 1)),
        "name": [f"user_{i}" for i in range(1, num_rows + 1)],
        "value": [i * 1.5 for i in range(1, num_rows + 1)],
    }
    table = pa.table(data)
    pq.write_table(table, parquet_file, row_group_size=row_group_size)
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

    def test_empty_file(self):
        self.parquet_file = make_parquet_file(num_rows=0)

        rows = list(sample_row_iterator(self.parquet_file, sample_rate=5, max_records=1000))

        self.assertEqual(rows, [])
