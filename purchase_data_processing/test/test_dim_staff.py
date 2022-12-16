from dim_tables import retrieve_table_from_s3_bucket_convert_dataframe
from moto import mock_s3
import boto3
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch
import pytest
from botocore.exceptions import ClientError


def test_return_correct_error_when_bucket_or_file_does_not_exist():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/')
        with pytest.raises(ClientError) as exc_info:
            retrieve_table_from_s3_bucket_convert_dataframe('fake_bucket', 'numbers')
        with pytest.raises(Exception) as exc_info:
            retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'stock')
        # assert str(exc_info.value) == 
        with pytest.raises(ClientError):
            retrieve_table_from_s3_bucket_convert_dataframe('fake_bucket', 'stock')
        
def test_return_correct_dataframe_when_table_exists_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/')
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        test_numbers_df = pd.read_csv('test_example.csv')
        assert_frame_equal(result, test_numbers_df)

# def test_return_correct_dataframe_when_correct_bucket():
#     with mock_s3():
#         s3 = boto3.client('s3', region_name='us-east-1')
#         s3.create_bucket(Bucket='test-bucket')ß
#         s3.upload_file('example.csv', 'test-bucket', 'numbers')
#         result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers')
#         assert_frame_equal(result, numbers_df)

        
# def test_return_latest_table_in_dataframe_when_multiple_table_exist_in_S3_bucket():
#     with mock_s3():
#         s3 = boto3.client('s3', region_name='us-east-1')
#         s3.create_bucket(Bucket='test-bucket')

#     pass

# def test_return_error_when_bucket_does_not_exist():
#     pass

# def test_return_error_when_file_does_not_exist():
#     pass
