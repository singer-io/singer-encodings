import unittest
from unittest import mock
from singer_encodings import json_schema

class Connection:
    def get_files(self, search_prefix, search_pattern):
        return [{'last_modified': "2020-01-01", 'filepath': 'root_dir/file1.csv'}, {'last_modified': "2020-01-02", 'filepath': 'root_dir/file2.csv'}]

@mock.patch("singer_encodings.json_schema.sample_files")
class TestEmptySchema(unittest.TestCase):
    def test_not_empty_samples(self, mocked_sample_files):
        mocked_sample_files.return_value = [{'a1': 'b1', 'a2': 'b2'}, {'a1': 'c1', 'a2': 'c2'}]
        conn = Connection()
        schema = json_schema.get_schema_for_table(conn, {"table_name":"data", "search_prefix":"/root_dir", "search_pattern":"test.*.csv"})
        schema_properties_key = schema.get("properties").keys()
        # for non-empty schema verify if headers values are persent in schema
        self.assertTrue('a1' in schema_properties_key)
        self.assertTrue('a2' in schema_properties_key)

    def test_empty_samples(self, mocked_sample_files):
        mocked_sample_files.return_value = []
        conn = Connection()
        schema = json_schema.get_schema_for_table(conn, {"table_name":"data", "search_prefix":"/root_dir", "search_pattern":"test.*.csv"})
        # for empty samples verify for schema is empty
        self.assertEqual({}, schema)
