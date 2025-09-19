import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from unittest import mock
from singer_encodings.parquet import get_row_iterator


class TestGetRowIterator(unittest.TestCase):
    def setUp(self):
        self.parquet_file = tempfile.TemporaryFile('w+b')
        data = {
            "id": list(range(1, 101)),  # integers 1–100
            "name": [f"user_{i}" for i in range(1, 101)],  # strings
            "value": [i * 1.5 for i in range(1, 101)],  # floats
        }
        table = pa.table(data)
        pq.write_table(table, self.parquet_file)

    def tearDown(self):
        self.parquet_file.close()

    def test(self):
        row_iterator = get_row_iterator(self.parquet_file)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})
        self.assertEqual(rows[99], {'id': 100, 'name': 'user_100', 'value': 150})
        self.assertEqual(len(rows), 100)
