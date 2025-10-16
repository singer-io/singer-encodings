import json
import unittest
import tempfile
from unittest import mock
from singer_encodings.jsonl import get_row_iterator


class TestGetRowIterator(unittest.TestCase):
    def setUp(self):
        self.jsonl_file = tempfile.TemporaryFile('w+')
        data = [{
            "id": i,
            "name": f"user_{i}",
            "value": i * 1.5
        }
        for i in range(1, 101)]
        for row in data:
            json.dump(row, self.jsonl_file)
            self.jsonl_file.write('\n')
        self.jsonl_file.seek(0)

    def tearDown(self):
        self.jsonl_file.close()

    def test(self):
        row_iterator = get_row_iterator(self.jsonl_file)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})
        self.assertEqual(rows[99], {'id': 100, 'name': 'user_100', 'value': 150})
        self.assertEqual(len(rows), 100)

class TestGetRowIteratorSkipsEmptyRows(unittest.TestCase):
    def setUp(self):
        self.jsonl_file = tempfile.TemporaryFile('w+')
        self.jsonl_file.write('\n\n\n')
        json.dump({'a': 1}, self.jsonl_file)
        self.jsonl_file.write('\n    \n\n     \n\t\n')
        json.dump({}, self.jsonl_file)
        self.jsonl_file.write('\n')
        self.jsonl_file.seek(0)

    def tearDown(self):
        self.jsonl_file.close()

    def test(self):
        row_iterator = get_row_iterator(self.jsonl_file)
        rows = [r for r in row_iterator]
        self.assertEqual(rows, [{'a': 1}])
