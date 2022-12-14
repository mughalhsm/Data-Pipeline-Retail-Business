from dim_tables import retrieve_table_from_s3_bucket_convert_to_dataframe
from moto import mock_s3
import boto3
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch

numbers_df = pd.read_csv('example.csv')

def test_return_correct_dataframe_when_correct_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('example.csv', 'test-bucket', 'numbers')
        result = retrieve_table_from_s3_bucket_convert_to_dataframe('test-bucket', 'numbers')
        assert_frame_equal(result, numbers_df)

        
def test_return_latest_table_in_dataframe_when_multiple_table_exist_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')

    pass

def test_return_error_when_bucket_does_not_exist():
    pass

def test_return_error_when_file_does_not_exist():
    pass
