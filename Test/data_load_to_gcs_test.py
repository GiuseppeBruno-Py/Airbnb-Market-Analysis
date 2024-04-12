import unittest
from unittest.mock import patch, call
import os
from src.data_load_to_gcs import upload_to_gcs  # Make sure the import path matches your project structure

class TestUploadToGCS(unittest.TestCase):
    @patch('src.data_load_to_gcs.storage.Client')
    @patch('src.data_load_to_gcs.glob.glob')
    @patch('src.data_load_to_gcs.os.path.join')
    def test_upload_to_gcs(self, mock_join, mock_glob, mock_storage_client):
        # Set up the mock objects
        mock_files = ['/path/to/Datasets/file1.txt', '/path/to/Datasets/file2.txt']
        mock_glob.return_value = mock_files  # Mock the return value of glob.glob to simulate files in a directory
        mock_join.return_value = '/path/to/Datasets/*'

        mock_bucket = mock_storage_client.return_value.bucket.return_value
        mock_blob = mock_bucket.blob.return_value  # Mock the blob method to simulate GCS blob operations

        # Call the function under test
        upload_to_gcs('dummy-bucket', '/path/to/Datasets')

        # Check if the bucket method was called correctly
        mock_storage_client.return_value.bucket.assert_called_with('dummy-bucket')

        # Verify if the upload method was called the correct number of times and with correct parameters
        self.assertEqual(mock_blob.upload_from_filename.call_count, len(mock_files))
        mock_blob.upload_from_filename.assert_any_call('/path/to/Datasets/file1.txt')
        mock_blob.upload_from_filename.assert_any_call('/path/to/Datasets/file2.txt')

        # Verify expected print statements using a mock for the print function
        expected_prints = [f'File {os.path.basename(f)} uploaded to {os.path.basename(f)} in bucket dummy-bucket.' for f in mock_files]
        with patch('builtins.print') as mocked_print:
            upload_to_gcs('dummy-bucket', '/path/to/Datasets')
            mocked_print.assert_has_calls([call(p) for p in expected_prints], any_order=True)

if __name__ == '__main__':
    unittest.main()
