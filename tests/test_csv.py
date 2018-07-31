import unittest
from singer_encodings import csv

class TestRestKey(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3,4"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], ['4'])

class TestNullBytes(unittest.TestCase):

    csv_data = [b"columnA,columnB\0,columnC", b"1,2,3,4"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

class TestOptions(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        with self.assertRaises(Exception):
            row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['fizz']})

        row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        with self.assertRaises(Exception):
            row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['fizz']})
