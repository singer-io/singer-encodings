import unittest
from unittest import mock
from singer_encodings import json_schema
from singer_encodings import csv
import csv as _csv
import io

class Connection:
    def get_file_handle(self, f):
        if f.get("raise_error"):
            raise OSError("OSError")
        elif f.get("raise_permission_error"):
            raise PermissionError("Permission denied")
        else:
            return mock.mock_open()

@mock.patch("singer_encodings.csv.get_row_iterators")
class TestSampleFile(unittest.TestCase):
    def test_positive(self, mocked_csv_row_iterator):
        mocked_csv_row_iterator.return_value = [_csv.DictReader(io.StringIO("header\nvalue"))]
        conn = Connection()
        empty_file, samples = json_schema.sample_file(conn, {"table_name": "data", "key_properties": ["id"], "delimiter": ","}, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, 1, 1000)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEqual(1, mocked_csv_row_iterator.call_count)
        # test if file is empty
        self.assertEqual(False, empty_file)
        # test if samples is not an empty list
        self.assertNotEqual([], samples)

    def test_negative_OSError(self, mocked_csv_row_iterator):
        conn = Connection()
        empty_file, samples = json_schema.sample_file(conn, {"table_name": "data", "key_properties": ["id"], "delimiter": ","}, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01", "raise_error": True}, 1, 1000)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEqual(0, mocked_csv_row_iterator.call_count)
        # test if file is empty
        self.assertEqual(False, empty_file)
        # test if samples is not an empty list
        self.assertEqual([], samples)

    def test_negative_PermisisonError(self, mocked_csv_row_iterator):
        conn = Connection()
        empty_file, samples = json_schema.sample_file(conn, {"table_name": "data", "key_properties": ["id"], "delimiter": ","}, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01", "raise_permission_error": True}, 1, 1000)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEqual(0, mocked_csv_row_iterator.call_count)
        # test if file is empty
        self.assertEqual(False, empty_file)
        # test if samples is not an empty list
        self.assertEqual([], samples)
