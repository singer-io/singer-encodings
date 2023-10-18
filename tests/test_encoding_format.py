import unittest
from unittest.mock import Mock
from singer_encodings.utils import is_valid_encoding

class TestIsValidEncoding(unittest.TestCase):
    def test_valid_encoding(self):

        # Test with a valid encoding format (utf-8)
        encoding_format = "utf-8"
        result = is_valid_encoding(encoding_format)

        # Assert that the function returns True for a valid encoding
        self.assertTrue(result)

    def test_invalid_encoding(self):

        # Test with an invalid encoding format (a non-existent format)
        encoding_format = "invalid_encoding"
        result = is_valid_encoding(encoding_format)

        # Assert that the function returns False for an invalid encoding
        self.assertFalse(result)
