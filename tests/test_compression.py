import unittest
from unittest.mock import patch, MagicMock, Mock
import zipfile
from io import BytesIO
from singer_encodings.compression import infer

class TestCompression(unittest.TestCase):

    @patch('singer_encodings.compression.zipfile.ZipFile')
    def test_valid_zip_file(self, mock_zipfile):
        # Mock a valid zip file
        mock_zip = MagicMock()
        mock_zip.namelist.return_value = ['file1.txt', 'file2.txt']
        mock_zip.open.side_effect = [BytesIO(b'content1'), BytesIO(b'content2')]
        mock_zipfile.return_value.__enter__.return_value = mock_zip

        # Mock the iterable
        iterable = MagicMock()

        results = list(infer(iterable, 'test.zip'))
        self.assertEqual(len(results), 2)
        mock_zip.namelist.assert_called_once()
        mock_zip.open.assert_any_call('file1.txt')
        mock_zip.open.assert_any_call('file2.txt')

    @patch('singer_encodings.compression.LOGGER.info')
    @patch('singer_encodings.compression.zipfile.ZipFile')
    def test_invalid_zip_file(self, mock_zipfile, mock_logger):

        # Mock the fallback file handle
        mock_iterable = Mock()
        mock_get_file_handle = Mock()
        mock_iterable.stat.return_value.st_size = 10 * 1024 * 1024
        mock_iterable.read.return_value = BytesIO(b'valid_zip_content').getvalue()
        mock_get_file_handle.get_file_handle.return_value = mock_iterable

        # Mock the fallback zip file
        mock_zip = MagicMock()
        mock_zip.namelist.return_value = ['file3.txt']
        mock_zip.open.side_effect = [BytesIO(b'content3')]
        
        # Simulate BadZipFile exception first and then Mock the fallback zip file
        mock_zipfile.side_effect = [zipfile.BadZipFile, mock_zip]
        results = list(infer(mock_iterable, 'test.zip', conn = mock_get_file_handle))

        mock_logger.assert_called_once_with("Failed to extract the ZIP file test.zip, attempting to load the entire file(size - 10.0 MB) into memory.")
