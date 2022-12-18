from purchase_data_processing.src.dim_tables import retrieve_table_from_s3_bucket_convert_dataframe, create_staff_dim_dataframe, create_counterparty_dim_dataframe, create_currency_dim_dataframe, create_location_dim_dataframe, create_date_dim_dataframe, create_fact_purchase_orders_dataframe, convert_dataframe_to_parquet_and_upload_S3
from moto import mock_s3
import boto3
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch
import pytest
import time
import numpy as np
from botocore.exceptions import ClientError

@pytest.mark.skip
def test_return_correct_dataframe_when_table_exists_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/')
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        expected_df = pd.read_csv('test_example.csv')
        assert_frame_equal(result, expected_df)

@pytest.mark.skip
def test_StringIO_component_of_retrieve_table_function_handles_missing_column_heading_values():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example_2.csv', 'test-bucket', 'TableName/numbers/')
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        expected_df = pd.read_csv('test_example_2.csv')
        assert_frame_equal(result, expected_df)

@pytest.mark.skip
def test_return_latest_table_in_dataframe_when_multiple_table_exist_in_S3_bucket():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        # Inserted 3 numbers tables into the S3 bucket.
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/612')
        s3.upload_file('test_example_2.csv', 'test-bucket', 'TableName/numbers/018')
        # Time added to ensure file above uploads before file below.
        time.sleep(1)
        s3.upload_file('test_example_3.csv', 'test-bucket', 'TableName/numbers/321')
        # Testing to see function retreive the files inserted most recently.
        result = retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'numbers' )
        expected_df = pd.read_csv('test_example_3.csv')
        assert_frame_equal(result, expected_df)

@pytest.mark.skip
def test_return_correct_error_when_bucket_does_not_exist():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/')
        with pytest.raises(ClientError):
            retrieve_table_from_s3_bucket_convert_dataframe('fake-bucket', 'numbers')

@pytest.mark.skip
def test_return_correct_error_when_file_does_not_exist():
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.upload_file('test_example.csv', 'test-bucket', 'TableName/numbers/')
        with pytest.raises(ValueError):
            retrieve_table_from_s3_bucket_convert_dataframe('test-bucket', 'stock')



@pytest.mark.skip
def test_return_correct_staff_dim_dataframe_when_given_correct_tables_as_argument_in_correct_order():
    staff_df = pd.read_csv('table_files/staff.csv')
    department_df = pd.read_csv('table_files/department.csv')
    staff_dim_df = create_staff_dim_dataframe(staff_df, department_df)
    list_column_names = list(staff_dim_df.columns)
    excepted_list_column_name = ['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']
    assert list_column_names == excepted_list_column_name
    assert len(staff_dim_df.index) == 20


@pytest.mark.skip
def test_return_correct_staff_dim_dataframe_when_given_correct_tables_as_argument_incorrect_order():
    staff_df = pd.read_csv('table_files/staff.csv')
    department_df = pd.read_csv('table_files/department.csv')
    staff_dim_df = create_staff_dim_dataframe(department_df, staff_df)
    list_column_names = list(staff_dim_df.columns)
    excepted_list_column_name = ['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']
    assert list_column_names == excepted_list_column_name
    assert len(staff_dim_df.index) == 20


@pytest.mark.skip
def test_create_staff_dim_throws_error_when_given_incorrect_datatype_as_argument():
    with pytest.raises(TypeError):
        create_staff_dim_dataframe("12", 123)


@pytest.mark.skip
def test_return_correct_counterparty_dim_dataframe_with_correct_arguements():
    counterparty_df = pd.read_csv('table_files/counterparty.csv')
    address_df = pd.read_csv('table_files/address.csv')
    counterparty_dim_df = create_counterparty_dim_dataframe(counterparty_df, address_df)
    list_column_names = list(counterparty_dim_df.columns)
    excepted_list_column_name = ['counterparty_id', 'counterparty_legal_name', 'counterparty_address_id',
       'counterparty_address_line_1', 'counterparty_address_line_2',
       'counterparty_district', 'counterparty_city',
       'counterparty_postal_code', 'counterparty_country',
       'counterparty_phone']
    assert list_column_names == excepted_list_column_name
    assert len(counterparty_dim_df.index) == 20


@pytest.mark.skip    
def test_create_counterparty_dim_throws_error_when_argumements_incorrect_order():
    counterparty_df = pd.read_csv('table_files/counterparty.csv')
    address_df = pd.read_csv('table_files/address.csv')
    with pytest.raises(KeyError):
        create_counterparty_dim_dataframe(address_df, counterparty_df)


@pytest.mark.skip
def test_create_counterparty_dim_throws_error_when_incorrect_arguements():
    counterparty_df = pd.read_csv('table_files/counterparty.csv')
    address_df = pd.read_csv('table_files/address.csv')
    with pytest.raises(TypeError):
        create_counterparty_dim_dataframe('Hello', True)


@pytest.mark.skip
def test_create_currency_dim_table_created_correctly():
    currency_df = pd.read_csv('table_files/currency.csv')
    currency_dim_df = create_currency_dim_dataframe(currency_df)
    list_column_names = list(currency_dim_df.columns)
    excepted_list_column_name = ['currency_id', 'currency_code', 'currency_name']
    currency_dataframe_values = currency_dim_df.values
    expected_values = [[1, 'GBP', 'Pound Sterling'], [2, 'USD', 'US Dollars'], [3, 'EUR', 'Euro']]
    assert list_column_names == excepted_list_column_name
    assert currency_dataframe_values.tolist() == expected_values


@pytest.mark.skip
def test_create_currency_dim__raises_error_incorrect_arguements():
    counterparty_df = pd.read_csv('table_files/counterparty.csv')
    with pytest.raises(KeyError):
        create_currency_dim_dataframe(counterparty_df)
    with pytest.raises(AttributeError):
        create_currency_dim_dataframe(12)


@pytest.mark.skip
def test_create_location_dim_table_created_correctly():
    address_df = pd.read_csv('table_files/address.csv')
    location_dim_df = create_location_dim_dataframe(address_df)
    list_column_names = list(location_dim_df.columns)
    excepted_list_column_name = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    assert list_column_names == excepted_list_column_name
    assert len(location_dim_df.index) == 30

@pytest.mark.skip
def test_create_location_dim__raises_error_incorrect_arguements():
    counterparty_df = pd.read_csv('table_files/counterparty.csv')
    with pytest.raises(AttributeError):
        create_currency_dim_dataframe(12)
    with pytest.raises(KeyError):
        create_location_dim_dataframe(counterparty_df)

@pytest.mark.skip
def test_create_date_dim_table_correctly():
   date_dim_df = create_date_dim_dataframe()
   date_dim_column_values = date_dim_df.columns.tolist()
   expected_column_values =  ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
   assert date_dim_column_values == expected_column_values

@pytest.mark.skip
def test_create_fact_purchase_order_table_correctly():
    purchase_order_df = pd.read_csv('table_files/purchase_order.csv')
    fact_purchase_order_df = create_fact_purchase_orders_dataframe(purchase_order_df)
    column_names = fact_purchase_order_df.columns.tolist()
    expected_column_names = ['purchase_order_id', 'staff_id', 'counterparty_id', 'item_code',
       'item_quantity', 'item_unit_price', 'currency_id',
       'agreed_delivery_date', 'agreed_payment_date',
       'agreed_delivery_location_id', 'created_date', 'created_time',
       'last_updated_date', 'last_updated_time']
    assert column_names == expected_column_names
    assert len(fact_purchase_order_df.index) > 1


def test_convert_dataframe_to_parquet_and_uplods_to_S3_bucket():
    with mock_s3():
        test_example_df = pd.read_csv('test_example_2.csv')
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='processed-data')
        convert_dataframe_to_parquet_and_upload_S3(test_example_df, 'processed-data', 'test-example')
        s3_object = s3.get_object(Bucket='processed-data', Key='test-example')
        print(s3_object)
        assert 1 == 2

        
    
