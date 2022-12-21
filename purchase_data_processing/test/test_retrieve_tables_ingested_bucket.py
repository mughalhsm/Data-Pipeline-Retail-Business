from purchase_data_processing.src.retrieve_tables_ingested_bucket import retrieve_table_from_s3_bucket_convert_dataframe

from moto import mock_s3
import boto3
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import time
from botocore.exceptions import ClientError


def test_return_correct_dataframe_when_table_exists_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_table_files/staff.csv', 'test-bucket', 'TableName/staff/')
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'staff' )
        expected_df = pd.read_csv('test_table_files/staff.csv')
        assert_frame_equal(result, expected_df)


def test_StringIO_component_of_retrieve_table_function_handles_missing_column_heading_values():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example_files/test_example_2.csv', 'test-bucket', 'TableName/numbers/')
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        expected_df = pd.read_csv('test_example_files/test_example_2.csv')
        assert_frame_equal(result, expected_df)


def test_return_latest_table_in_dataframe_when_multiple_table_exist_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        # Inserted 3 numbers tables into the S3 bucket.
        s3.upload_file('test_example_files/test_example.csv', 'test-bucket', 'TableName/numbers/612')
        s3.upload_file('test_example_files/test_example_2.csv', 'test-bucket', 'TableName/numbers/018')
        # Time added to ensure file above uploads before file below.
        time.sleep(1)
        s3.upload_file('test_example_files/test_example_3.csv', 'test-bucket', 'TableName/numbers/321')
        # Testing to see function retreive the files inserted most recently.
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        expected_df = pd.read_csv('test_example_files/test_example_3.csv')
        assert_frame_equal(result, expected_df)


def test_return_correct_error_when_bucket_does_not_exist():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example_files/test_example.csv', 'test-bucket', 'TableName/numbers/')
        with pytest.raises(ClientError):
            retrieve_table_from_s3_bucket_convert_dataframe('fake-bucket', 'numbers')


def test_return_correct_error_when_file_does_not_exist():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_table_files/staff.csv', 'test-bucket', 'TableName/staff/')
        with pytest.raises(ValueError):
            retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'address')


