from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3
import pandas as pd
import boto3
import time
from moto import mock_s3
from pandas.testing import assert_frame_equal

def test_convert_dataframe_to_parquet_and_uplods_to_S3_bucket():
    with mock_s3():
        test_staff_df = pd.read_csv('test_table_files/staff.csv')
        test_currency_df = pd.read_csv('test_table_files/currency.csv')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='processed-data')
        convert_dataframe_to_parquet_and_upload_S3(test_staff_df, 'processed-data', 'staff')
        time.sleep(1)
        convert_dataframe_to_parquet_and_upload_S3(test_currency_df, 'processed-data', 'currency')
        response = s3.list_objects_v2(Bucket='processed-data')
        list_of_contents = []
        for filename in response['Contents']:
            list_of_contents.append(filename['Key'])
        assert ['currency.parquet', 'staff.parquet'] == list_of_contents

def test_ensure_upload_file_is_parquet_file():
    with mock_s3():
        test_address_df = pd.read_csv('test_table_files/address.csv')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='processed-data')
        # Converting dataframe to parquet file and uploading to S3 bucket. 
        convert_dataframe_to_parquet_and_upload_S3(test_address_df, 'processed-data', 'address')
        time.sleep(1)
        ## Downloading this uploaded parquet locally
        s3.download_file('processed-data', 'address.parquet', 'test_downloaded_parquet_file.parquet')
        # Converting download parquet file which is stored locally to a dataframe. 
        download_df = pd.read_parquet('test_downloaded_parquet_file.parquet', engine='fastparquet')
        # Testing the the dataframe created from the downloand parquet file from S3 is equal to the orgical dataframe created by local CSV file. 
        assert_frame_equal(test_address_df, download_df)

