import unittest
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from unittest import mock
from singer_encodings.parquet import get_row_iterator, get_schema


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
        self.assertEqual(len(rows), 100)

class TestGetSchema(unittest.TestCase):
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
        schema = get_schema(self.parquet_file, {'stream_name': 'jeffrey'})
        self.assertEqual(schema, {
            'type': 'SCHEMA',
            'stream': 'jeffrey',
            'schema': {
                'type': ['null', 'object'],
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'name': {'type': ['null', 'string']},
                    'value': {'type': ['null', 'number']}
                }
            }
        })

class TestNestedDataSchema(unittest.TestCase):
    def setUp(self):
        self.parquet_file = tempfile.TemporaryFile('w+b')
        table = pa.Table.from_pylist([{
            'id': 1,
            'name': 'nombre',
            'nestedlst': [{
                'id': 'foo'
            }, {
                'id':'bar'
            }],
            'nestedobj':{
                'element':'ohno!',
                'doublenested': {'foo':'bar'}
            }
        }])

        pq.write_table(table, self.parquet_file)

    def tearDown(self):
        self.parquet_file.close()

    def test(self):
        schema = get_schema(self.parquet_file, {'stream_name': 'jill'})
        self.assertEqual(schema, {
            'type': 'SCHEMA',
            'stream': 'jill',
            'schema': {
                'type': ['null', 'object'],
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'name': {'type': ['null', 'string']},
                    'nestedlst': {
                        'type': ['null', 'array'],
                        'items': {
                            'type': ['null', 'object'],
                            'properties': {'id': {'type': ['null', 'string']}}
                        }
                    },
                    'nestedobj': {
                        'type': ['null', 'object'],
                        'properties': {
                            'doublenested': {
                                'type': ['null', 'object'],
                                'properties': {
                                    'foo': {'type': ['null', 'string']}
                                }
                            },
                            'element': {'type': ['null', 'string']}
                        }
                    }
                }
            }
        })
