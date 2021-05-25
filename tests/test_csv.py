import unittest
from unittest import mock
from singer_encodings.csv_helper import CSVHelper
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
        self.assertEqual(rows[0]['columnB'], '2')


class TestOptionsWithDuplicateHeaders(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        try:
            row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['fizz']})
        except Exception as ex:
            expected_message = "CSV file missing required headers: {'fizz'}"
            self.assertEquals(expected_message, str(ex))

        row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']}, headers_in_catalog=None, with_duplicate_headers=True)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        try:
            row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']}, headers_in_catalog=None, with_duplicate_headers=True)
        except Exception as ex:
            expected_message = "CSV file missing date_overrides headers: {'fizz'}"
            self.assertEquals(expected_message, str(ex))


class TestOptions(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        try:
            row_iterator = csv.get_row_iterator(self.csv_data, options={'key_properties': ['fizz']})
        except Exception as ex:
            expected_message = "CSV file missing required headers: {'fizz'}"
            self.assertEquals(expected_message, str(ex))

        row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']})
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['columnA'], '1')

        try:
            row_iterator = csv.get_row_iterator(self.csv_data, options={'date_overrides': ['columnA']})
        except Exception as ex:
            expected_message = "CSV file missing date_overrides headers: {'fizz'}"
            self.assertEquals(expected_message, str(ex))

class TestRestKeyWithDuplicateHeader(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2,3,4"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], [{"no_headers": ["4"]}])

class TestRestValue(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC", b"1,2"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB"])

class TestDuplicateHeaders(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5,6"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], [{"columnB": "4"},{"columnC": ["5", "6"]}])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestKey(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5,6,7,8,9"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'],  [{"no_headers": ["7", "8", "9"]},{"columnB": "4"},{"columnC": ["5", "6"]}])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestValue(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(rows[0]['_sdc_extra'], [{"columnB": "4"},{"columnC": "5"}])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC","_sdc_extra"])

class TestDuplicateHeadersRestValueNoSDCExtra(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC"])

class TestMissingFiedInCatalog(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnB,columnC,columnC", b"1,2,3,4,5,6"]

    def test(self):
        row_iterator = csv.get_row_iterator(self.csv_data, None, ["columnA","columnB"], True)
        rows = [r for r in row_iterator]
        self.assertListEqual(rows[0]['_sdc_extra'], [{"columnC": ["3", "5", "6"]},{"columnB": "4"}])
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","_sdc_extra"])


class TestWarningForDupHeaders(unittest.TestCase):

    csv_data = [b"columnA,columnB,columnC,columnC", b"1,2,3"]

    @mock.patch("singer_encodings.csv_helper.LOGGER.warn")
    def test(self, mocked_logger_warn):
        row_iterator = csv.get_row_iterator(self.csv_data, None, None, True)
        rows = [r for r in row_iterator]
        self.assertEqual(list(rows[0].keys()), ["columnA","columnB","columnC"])

        mocked_logger_warn.assert_called_with('Duplicate Header(s) %s found in the csv and its value will be stored in the \"_sdc_extra\" field.', {'columnC'})