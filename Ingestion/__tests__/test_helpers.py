import boto3
import json
import pytest
from moto import mock_secretsmanager, mock_s3
from src.Helpers import get_credentials, put_into_bucket, delete_TESTFUNC_last_run_num_object
from botocore.exceptions import ClientError
import unittest
import time

@pytest.fixture
def secrets_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")



def test_that_returns_correct_credentials_from_a_secret(secrets_client):
    #Create a secret in the mock secrets manager service
    secrets_client.create_secret(
        Name="totesys_credentials",
        SecretString='{"user": "test_user", "password": "test_pass", "host": "test_host"}'
    )

    #Call the get_credentials() function with the secret name
    username, passw, host = get_credentials('totesys_credentials')

    #Assert that the function returns the expected credentials
    assert username == "test_user"
    assert passw == "test_pass"
    assert host == "test_host"        



def test_that_entering_wrong_credentials_name_raises_error(secrets_client):
    secrets_client.create_secret(
        Name="totesys_credentials",
        SecretString='{"user": "test_user", "password": "test_pass", "host": "test_host"}'
    )

    with pytest.raises(ClientError) as ce:
        get_credentials('totsys_credentials')

    assert 'ResourceNotFound' in str(ce)



@mock_s3
def test_put_into_buckets():
    #Test that putBuckets will upload file into bucket with correct formatting
    test_table_name='TESTtable_name'
    test_increment='TESTincrement'
    test_bucket_name='TESTbucket'
    test_dataframe_as_csv='test,1'
    
    s3get_conn = boto3.resource('s3', region_name='us-east-1')
    s3conn = boto3.client('s3', region_name='us-east-1')

    s3conn.create_bucket(Bucket=test_bucket_name)

    put_into_bucket(bucket_name=test_bucket_name, table_name=test_table_name,
    increment=test_increment, dataframe_as_csv=test_dataframe_as_csv)

    response_list = s3conn.list_objects_v2(Bucket=test_bucket_name)

    body = s3get_conn.Object(test_bucket_name, f"TableName/{test_table_name}/RunNum:{test_increment}.csv").get()[
        'Body'].read().decode("utf-8")


    assert f"TableName/{test_table_name}/RunNum:{test_increment}.csv" in response_list['Contents'][0]['Key']

@pytest.mark.skip
@mock_s3
def test_can_be_called_multiple_times_and_store_multiple_objects():
    '''Tests 2 things, 1: can store multiple things in a bucket
    2: can delete the last item using another function 
    which would be invoked if ingestion failure'''
    count=0
    test_bucket_name=f'TESTbucket'
    tables=['table1', 'table2','table3', 'table4']
    s3conn = boto3.client('s3', region_name='us-east-1')

    s3conn.create_bucket(Bucket=test_bucket_name)

    for s in tables:
        count+=1
        test_table_name=f'TESTtable_name{count}'
        test_increment=f'TESTincrement{count}'
        test_dataframe_as_csv=f'test,0{count}'

        put_into_bucket(bucket_name=test_bucket_name, table_name=test_table_name,
        increment=test_increment, dataframe_as_csv=test_dataframe_as_csv)
        time.sleep(1)

    response_list = s3conn.list_objects_v2(Bucket=test_bucket_name)
    assert len(response_list['Contents']) == 4

    delete_TESTFUNC_last_run_num_object(bucket_name=test_bucket_name)

    response_list2 = s3conn.list_objects_v2(Bucket=test_bucket_name)
    assert len(response_list2['Contents']) == 3
    assert 'TableName/TESTtable_name4/RunNum:TESTincrement4.csv' not in response_list2['Contents']
