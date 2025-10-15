import unittest
from fastavro import reader, writer, parse_schema
import tempfile
from unittest import mock
from singer_encodings.avro import get_row_iterator


class TestGetRowIterator(unittest.TestCase):
    def setUp(self):
        self.avro_file = tempfile.TemporaryFile('w+b')
        data = [{
            "id": i,
            "name": f"user_{i}",
            "value": i * 1.5
        }
        for i in range(1, 101)]

        schema = {
            'name': 'Test Data',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'id', 'type': 'int'},
                {'name': 'name', 'type': 'string'},
                {'name': 'value', 'type': 'double'},
            ],
        }
        parsed_schema = parse_schema(schema)

        writer(self.avro_file, parsed_schema, data)

    def tearDown(self):
        self.avro_file.close()

    def test(self):
        self.avro_file.seek(0)
        row_iterator = get_row_iterator(self.avro_file)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0], {'id': 1, 'name': 'user_1', 'value': 1.5})
        self.assertEqual(rows[99], {'id': 100, 'name': 'user_100', 'value': 150})
        self.assertEqual(len(rows), 100)
