# Movies-Data-ETL

The goal of this ETL process is to load data into a Google Cloud Storage Bucket. 

## The steps are:
1. Download ratings and movies data from https://files.grouplens.org/datasets/movielens.
2. Create a Google Storage Bucket.
3. Process the Data and Export to CSV.
4. Load CSV files into the Storage Bucket.

## Pre-requisites
1. To run the ETL process you must enable the API for the services: Cloud Storage & Cloud Storage JSON API.
2. Create a Service Account --> Create a Private Key --> Download Key Configurations in a JSON file, name it ServiceKey_GoogleCloud.json and put it in the project folder.
  - You can find an example of this file in the repository(I wont show my actual private key details).

## Applying The ETL Process
1. Download Data:
  - bash download_data.sh
2. Run the ETL process
  - python movies_etl.py

## End Result

![image](https://user-images.githubusercontent.com/65648983/192589738-4296b410-48b6-44f9-93f0-64b26e459fd8.png)

