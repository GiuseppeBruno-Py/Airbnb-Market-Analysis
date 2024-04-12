Certainly! Here's how you can integrate the missing explanations about the necessity of data transformations and the decision not to use partitioning or clustering in BigQuery into your README:

---

# Project Title

## Overview
This project demonstrates an end-to-end data pipeline from data ingestion to visualization, focusing on real estate datasets. It involves enriching geolocation data and performing transformations using PySpark before loading the processed data into Google BigQuery for analysis and visualization through a dashboard in Looker Studio.

## Problem Description
The project addresses a significant challenge with the raw geolocation data, where a substantial portion of the dataset—specifically, 37,722 out of 49,311 records—lacked street name information. Given the high number of missing entries, even when processing was limited to the first 1,000 lines, there were still 704 records with null street names. This enrichment and transformation were crucial to provide complete and usable data for further analysis and decision-making.

## Architecture
The data pipeline follows these steps:
1. **Data Ingestion:** Data is ingested from the Kaggle dataset and stored in the `datasets` folder.
2. **Storage:** Data is uploaded to Google Cloud Storage for further processing.
3. **Data Enrichment:** Using Google's Geolocation API, the datasets are enriched with street names based on latitude and longitude information.
4. **Data Transformation:** Transformed and enriched data is processed with PySpark in the `data_transformation.py` script.
5. **Loading Data:** The processed data is loaded into Google BigQuery.
6. **Orchestration:** Apache Airflow manages the workflow, with the DAG configured in the `kaggle_to_gcs` script.
7. **Testing:** Unit tests for the ingestion and loading stages are in the `test` folder.

## Resources and Documentation
### Architecture Diagram
Below is the architecture diagram which illustrates the entire data flow:

![Architecture](https://github.com/GiuseppeBruno-Py/Airbnb-Market-Analysis/assets/91219935/a72fd59d-fedf-45c2-bce3-dd030dd12021)

### SQL Queries
The SQL queries used for dimensional modeling are located in the `queries` folder. Each file details the construction of a specific dimension or fact table.

### Dashboard
Visualize the insights from the data through our Looker Studio dashboard:
[View Dashboard](https://lookerstudio.google.com/reporting/1a13f585-f2c8-4d6d-b69c-e6d9d35b14ed)

![Dashboard Screenshot](![Dashboard](https://github.com/GiuseppeBruno-Py/Airbnb-Market-Analysis/assets/91219935/4bc70c90-58f2-405a-9065-119675e25ed9))

## Data Modeling in BigQuery
The dimensional modeling in Google BigQuery includes:
- **Dim_Amenities**
- **Dim_Geolocation**
- **Dim_Property_Details**
- **Dim_Time**
- **Fact_Property_Performance**

Given the lightweight nature of the database, there was no need to implement clustering or partitioning. The base performance was already optimized, allowing for efficient query execution without additional configuration.


## Getting Started
### Prerequisites
- Google Cloud Storage
- Google BigQuery
- Apache Airflow
- Looker Studio
- PySpark environment

### Installation
1. Set up Google Cloud Storage and Google BigQuery.
2. Install Apache Airflow and configure the DAGs.
3. Prepare the PySpark environment for data processing.

### Usage
1. Run the `data_ingestion.py` to ingest data into the `datasets` folder.
2. Execute `data_transformation.py` for processing and enriching the data.
3. Use Apache Airflow to orchestrate the entire workflow.

### Testing
Run the following commands to execute unit tests:
```bash
python -m unittest test/data_ingestion_test.py
python -m unittest test/data_load_to_gcs_test.py
```

## Contributions
Contributions are welcome. Please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
