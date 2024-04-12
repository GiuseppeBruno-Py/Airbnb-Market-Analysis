import unittest
from unittest.mock import patch
from src.data_ingestion import download_dataset


class TestDownloadDataset(unittest.TestCase):
    @patch('src.data_ingestion.KaggleApi')  
    def test_download_dataset(self, mock_kaggle_api):
        # Create a mock instance of the API
        mock_api_instance = mock_kaggle_api.return_value
        
        # Call the function under test
        download_dataset()
        
        # Verify that authenticate was called once on the mock API instance
        mock_api_instance.authenticate.assert_called_once()
        
        # Verify that dataset_download_files was called correctly
        mock_api_instance.dataset_download_files.assert_called_once_with(
            'computingvictor/zillow-market-analysis-and-real-estate-sales-data',
            path='./Datasets/',
            unzip=True
        )

if __name__ == '__main__':
    unittest.main()  # Run the tests
