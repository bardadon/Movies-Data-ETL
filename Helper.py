import os 
from google.cloud import storage

import pandas as pd
from os import read
import numpy as np


class google_bucket:

    # Creating an Environmental Variable for the service key configuration
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

    # Creating a storage client
    storage_client = storage.Client()


    def __init__(self):
        pass


    def create_bucket(self, bucket_name, location = 'US'):

        '''
        Create a Google Cloud Stroge Bucket.

        Args:
            - bucket_name(str) - Name of the Bucket.
            - location(str, default = US) - Location.

        Returns:
            - True or False
        '''

        self.bucket_name = bucket_name
        self.location = location

        # Set bucket name and location
        bucket = self.storage_client.bucket(bucket_name=bucket_name)
        bucket.location = location

        try:
            # Create bucket
            bucket = self.storage_client.create_bucket(bucket)
            print('\nBucket Created Sucessfuly')
            print('Printing Bucket Details...\n')
            return print(vars(bucket))

        except Exception as e:
            print(e)


    def process_ratings(self, file_path = '/projects/project3/ml-100k/u.data'):

        '''
        Create Dataframe of movie ratings from the file u.data.

        Args:
            - file_path(str) - Path to u.data.

        Returns:
            - ratings(DataFrame)
        '''

        # Create a DatFrame for Movie Ratings
        ratings = pd.read_csv(file_path, delimiter='\t', header=None, names=['user_id','item_id', 'rating' ,'timestamp'])
        return ratings

    def process_movies(self, file_path = '/projects/project3/ml-100k/u.item'):

        '''
        Create Dataframe of movie names from the file u.item.

        Args:
            - file_path(str) - Path to u.item.

        Returns:
            - movies_df(DataFrame)
        '''

        self.file_path = file_path

        # Create a DataFrame for Movie Names
        with open(file_path, 'r', encoding="ISO-8859-1") as read_file:
            
            counter = 0
            movies_df = pd.DataFrame(columns=['item_id', 'movie_name', 'release_timestamp'])

            # Iterate thorugh the lines in the file
            for line in read_file:

                # From each line extract the first three values
                fields = line.split('|')
                item_id, movie_name, release_timestamp = fields[0], fields[1], fields[2]
                movie_name = movie_name[0:len(movie_name) - len(' (1234)')]

                # Aggerate line data
                line_data = [int(item_id), str(movie_name), release_timestamp]

                # Create a temp dataframe and append it to movies_df
                temp_df = pd.DataFrame(data=[line_data], columns=['item_id', 'movie_name', 'release_timestamp'])
                movies_df = pd.concat([temp_df, movies_df], ignore_index=True)

                counter += 1

            # Sort Values by item id
            movies_df.sort_values(by='item_id', ascending=True, inplace=True)

        # Close file
        read_file.close()

        return movies_df


    def export_dataframe_to_csv(self, dataframe, csv_name):

        '''
        Export DataFrame to CSV.

        Args:
            - dataframe(Pandas.DataFrame) = Name of the DataFrame to Export.
            - csv_name(str) = Name of the CSV file
        Returns:
            - None
        '''

        self.dataframe = dataframe
        self.csv_name = csv_name

        try:
            # Export to CSV
            dataframe.to_csv(csv_name + '.csv', index=False)
        except Exception as e:
            print(e)


    def load_data(self, blob_path, file_path, bucket_name):

        '''
        Load CSV files to Google Cloud Storage.

        Args:
            - blob_path(str) = Where the csv file should go in the bucket.
            - file_path(str) = Path to the CSV file
            - bucket_name(str) = Bucket to Load data into

        Returns:
            - True/False
        '''

        self.location_in_bucket = blob_path
        self.file_path = file_path
        self.bucket_name = bucket_name
        
        try:
            # Access bucket
            bucket = self.storage_client.get_bucket(bucket_name)

            # Create a blob from the bucket
            blob = bucket.blob(blob_name=blob_path)

            # Upload file
            blob.upload_from_filename(file_path)
            return True

        except Exception as e:
            print(e)
            return False
