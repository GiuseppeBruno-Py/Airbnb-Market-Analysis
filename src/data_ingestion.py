# Import the Kaggle API class from the extended Kaggle API module
from kaggle.api.kaggle_api_extended import KaggleApi

def download_dataset():
    # Create an instance of the KaggleApi class
    api = KaggleApi()
    
    # Authenticate the user with Kaggle using API credentials from ~/.kaggle/kaggle.json
    api.authenticate()
    
    # Define the specific dataset to download using its unique identifier
    dataset = "computingvictor/zillow-market-analysis-and-real-estate-sales-data"
    
    # Specify the local directory where the dataset will be downloaded and extracted
    path = './Datasets/'
    
    # Download and automatically unzip the dataset files to the specified path
    api.dataset_download_files(dataset, path=path, unzip=True)

# Execute the download function if this script is run as the main program
if __name__ == "__main__":
    download_dataset()
