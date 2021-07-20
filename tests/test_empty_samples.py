import unittest
from unittest import mock
from singer_encodings import json_schema

class Connection:
    def get_files(self, search_prefix, search_pattern):
        return [{'last_modified': "2020-01-01", 'filepath': 'root_dir/file.csv'}]

@mock.patch("singer_encodings.json_schema.sample_files")
class TestEmptySamples(unittest.TestCase):
    def test_not_empty_samples(self, mocked_sample_files):
        mocked_sample_files.return_value = [{'a1': 'b1', 'a2': 'b2'}]
        conn = Connection()
        schema = json_schema.get_schema_for_table(conn, {"table_name":"data", "search_prefix":"/root_dir", "search_pattern":"test.*.csv"})
        self.assertTrue('a1' in schema.get("properties").keys())

    def test_empty_samples(self, mocked_sample_files):
        mocked_sample_files.return_value = []
        conn = Connection()
        schema = json_schema.get_schema_for_table(conn, {"table_name":"data", "search_prefix":"/root_dir", "search_pattern":"test.*.csv"})
        self.assertEquals({}, schema)
