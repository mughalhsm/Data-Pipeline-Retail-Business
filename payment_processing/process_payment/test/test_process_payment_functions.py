import boto3
import os
import pytest
from moto import mock_s3
from tempfile import NamedTemporaryFile
from unittest.mock import Mock #
import pandas as pd
from pandas.testing import assert_frame_equal


from process_payment.src.process_payment_functions import (
    payment_files_list, payment_type_files_list, transaction_files_list,
    currency_files_list, counterparty_files_list, address_files_list,
    df_list_payment, df_list_payment_type, df_list_transaction,
    df_list_currency, df_list_counterparty, df_list_address,
    payment_files_processed_list, payment_type_files_processed_list,
    transaction_files_processed_list, currency_files_processed_list,
    counterparty_files_processed_list, date_files_processed_list,
    fact_payment_tables, dim_payment_type_tables, dim_transaction_tables,
    dim_currency_tables, dim_counterparty_tables, dim_date_tables)


"""Setting up mock credentials"""
@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        yield s3_client




"""Testing pulling files from the bucket"""
@mock_s3
def test_payment_files_list(s3_client):
    """Makes a list of all of the payment files (files with the
            TableName/payment prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/payment/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file3.csv")

    result = payment_files_list("my-test-bucket")

    assert result == ["TableName/payment/Run0001/test_file.csv",
                            "TableName/payment/Run0001/test_file2.csv"]


@mock_s3
def test_payment_type_files_list(s3_client):
    """Makes a list of all of the payment_type files (files with the
            TableName/payment_type prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/payment_type/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment_type/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file3.csv")

    result = payment_type_files_list("my-test-bucket")

    assert result == ["TableName/payment_type/Run0001/test_file.csv",
                            "TableName/payment_type/Run0001/test_file2.csv"]


@mock_s3
def test_transaction_files_list(s3_client):
    """Makes a list of all of the transaction files (files with the
            TableName/transaction prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/currency/Run0001/test_file3.csv")

    result = transaction_files_list("my-test-bucket")

    assert result == ["TableName/transaction/Run0001/test_file.csv",
                            "TableName/transaction/Run0001/test_file2.csv"]


@mock_s3
def test_currency_files_list(s3_client):
    """Makes a list of all of the currency files (files with the
            TableName/currency prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/currency/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/currency/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file3.csv")

    result = currency_files_list("my-test-bucket")

    assert result == ["TableName/currency/Run0001/test_file.csv",
                            "TableName/currency/Run0001/test_file2.csv"]


@mock_s3
def test_counterparty_files_list(s3_client):
    """Makes a list of all of the counterparty files (files with the
            TableName/counterparty prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/counterparty/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/counterparty/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file3.csv")

    result = counterparty_files_list("my-test-bucket")

    assert result == ["TableName/counterparty/Run0001/test_file.csv",
                            "TableName/counterparty/Run0001/test_file2.csv"]


@mock_s3
def test_address_files_list(s3_client):
    """Makes a list of all of the address files (files with the
            TableName/address prefix) within the bucket in order"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")

        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/address/Run0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/address/Run0001/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/Run0001/test_file3.csv")

    result = address_files_list("my-test-bucket")

    assert result == ["TableName/address/Run0001/test_file.csv",
                            "TableName/address/Run0001/test_file2.csv"]





"""Testing compilation of dataframes"""
@mock_s3
def test_df_list_payment(s3_client):
    """Converts list of payment filenames into list of payment dataframes with
                correct data"""
    
    file_text = """payment_id,created_at,last_updated,transaction_id,counterparty_id,payment_amount,currency_id,payment_type_id,paid,payment_date,company_ac_number,counterparty_ac_number
                2,2022-11-03 14:20:52.187,2022-11-03 14:20:52.187,2,15,552548.62,2,3,False,2022-11-04,67305075,31622269"""
    
    test_df = pd.DataFrame({
                            "payment_id": [2],
                            "created_at": ["2022-11-03 14:20:52.187"],
                            "last_updated": ["2022-11-03 14:20:52.187"],
                            "transaction_id": [2],
                            "counterparty_id": [15],
                            "payment_amount": [552548.62],
                            "currency_id": [2],
                            "payment_type_id": [3],
                            "paid": [False],
                            "payment_date": ["2022-11-04"],
                            "company_ac_number": [67305075],
                            "counterparty_ac_number": [31622269]
                            })

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment/test_file.csv")

        result = df_list_payment(["TableName/payment/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")

        assert_frame_equal(test_df, result[0])
        


@mock_s3
def test_df_list_payment_type(s3_client):
    """Converts list of payment_type filenames into list of payment_type
            dataframes with correct data"""
    
    file_text = """payment_type_id,payment_type_name,created_at,last_updated
    1,SALES_RECEIPT,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    2,SALES_REFUND,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    3,PURCHASE_PAYMENT,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    4,PURCHASE_REFUND,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""
    
    test_df = pd.DataFrame({
                            "payment_type_id": [1, 2, 3, 4],
                            "payment_type_name": ["SALES_RECEIPT", "SALES_REFUND", "PURCHASE_PAYMENT", "PURCHASE_REFUND"],
                            "created_at": ["2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"],
                            "last_updated": ["2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"]
                            })

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment_type/test_file.csv")

        result = df_list_payment_type(["TableName/payment_type/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")
        
        assert_frame_equal(test_df, result[0])


@mock_s3
def test_df_list_transaction(s3_client):
    """Converts list of transaction filenames into list of transaction
            dataframes with correct data"""
    
    file_text = """transaction_id,transaction_type,sales_order_id,purchase_order_id,created_at,last_updated
    1,PURCHASE,2.0,2.0,2022-11-03 14:20:52.186,2022-11-03 14:20:52.186
    2,PURCHASE,3.0,3.0,2022-11-03 14:20:52.187,2022-11-03 14:20:52.187
    3,SALE,1.0,1.0,2022-11-03 14:20:52.186,2022-11-03 14:20:52.186"""
    
    test_df = pd.DataFrame({
                            "transaction_id": [1, 2, 3],
                            "transaction_type": ["PURCHASE", "PURCHASE", "SALE"],
                            "sales_order_id": [2.0, 3.0, 1.0],
                            "purchase_order_id": [2.0, 3.0, 1.0],
                            "created_at": ["2022-11-03 14:20:52.186", "2022-11-03 14:20:52.187", "2022-11-03 14:20:52.186"],
                            "last_updated": ["2022-11-03 14:20:52.186", "2022-11-03 14:20:52.187", "2022-11-03 14:20:52.186"]
                            })

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/transaction/test_file.csv")

        result = df_list_transaction(["TableName/transaction/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")
        
        assert_frame_equal(test_df, result[0])


@mock_s3
def test_df_list_currency(s3_client):
    """Converts list of currency filenames into list of currency dataframes
            with correct data"""
    
    file_text = """currency_id,currency_code,created_at,last_updated
    1,GBP,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    2,USD,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    3,EUR,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""
    
    test_df = pd.DataFrame({
                            "currency_id": [1, 2, 3],
                            "currency_code": ["GBP", "USD", "EUR"],
                            "created_at": ["2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"],
                            "last_updated": ["2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962", "2022-11-03 14:20:49.962"]
                            })  

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/currency/test_file.csv")

        result = df_list_currency(["TableName/currency/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")
        
        assert_frame_equal(test_df, result[0])


@mock_s3
def test_df_list_counterparty(s3_client):
    """Converts list of counterparty filenames into list of counterparty
            dataframes with correct data"""
    
    file_text = """counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated
    1,Fahey and Sons,15,Michael Toy,Mrs. Lucy Runolfsdottir,2022-11-03 14:20:51.563,2022-11-03 14:20:51.563"""
    
    test_df = pd.DataFrame({
                            "counterparty_id": [1],
                            "counterparty_legal_name": ["Fahey and Sons"],
                            "legal_address_id": [15],
                            "commercial_contact": ["Michael Toy"],
                            "delivery_contact": ["Mrs. Lucy Runolfsdottir"],
                            "created_at": ["2022-11-03 14:20:51.563"],
                            "last_updated": ["2022-11-03 14:20:51.563"]
                            })    
                            
    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/counterparty/test_file.csv")

        result = df_list_counterparty(["TableName/counterparty/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")
        assert_frame_equal(test_df, result[0])


@mock_s3
def test_df_list_address(s3_client):
    """Converts list of address filenames into list of address dataframes with
                    correct data"""
    
    file_text = """address_id,address_line_1,address_line_2,district,city,postal_code,country,phone,created_at,last_updated
    1,6826 Herzog Via, ,Avon,New Patienceburgh,28441,Turkey,1803 637401,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""
    
    test_df = pd.DataFrame({
                            "address_id": [1],
                            "address_line_1": ["6826 Herzog Via"],
                            "address_line_2": [" "],
                            "district": ["Avon"],
                            "city": ["New Patienceburgh"],
                            "postal_code": [28441],
                            "country": ["Turkey"],
                            "phone": ["1803 637401"],
                            "created_at": ["2022-11-03 14:20:49.962"],
                            "last_updated": ["2022-11-03 14:20:49.962"]
                            })

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/address/test_file.csv")

        result = df_list_address(["TableName/address/test_file.csv"],
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")
        
        assert_frame_equal(test_df, result[0])





"""Testing count and listing of files in processed bucket"""
@mock_s3
def test_payment_files_processed_list(s3_client):
    """Counts number of fact_payment files in processed bucket and makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = payment_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/fact_payment/fact_payment_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/fact_payment/fact_payment_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/fact_payment/fact_payment_0003/test_file3.csv")

    result = payment_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/fact_payment/fact_payment_0001/test_file2.csv",
                            "Payment/fact_payment/fact_payment_0002/test_file.csv",
                            "Payment/fact_payment/fact_payment_0003/test_file3.csv"]



@mock_s3
def test_payment_type_files_processed_list(s3_client):
    """Counts number of dim_payment_type files in processed bucket, makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = payment_type_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/dim_payment_type/dim_payment_type_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/dim_payment_type/dim_payment_type_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/dim_payment_type/dim_payment_type_0003/test_file3.csv")

    result = payment_type_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/dim_payment_type/dim_payment_type_0001/test_file2.csv",
                            "Payment/dim_payment_type/dim_payment_type_0002/test_file.csv",
                            "Payment/dim_payment_type/dim_payment_type_0003/test_file3.csv"]



@mock_s3
def test_transaction_files_processed_list(s3_client):
    """Counts number of dim_transaction files in processed bucket, makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = transaction_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/dim_transaction/dim_transaction_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/dim_transaction/dim_transaction_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/dim_transaction/dim_transaction_0003/test_file3.csv")

    result = transaction_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/dim_transaction/dim_transaction_0001/test_file2.csv",
                            "Payment/dim_transaction/dim_transaction_0002/test_file.csv",
                            "Payment/dim_transaction/dim_transaction_0003/test_file3.csv"]



@mock_s3
def test_currency_files_processed_list(s3_client):
    """Counts number of dim_currency files in processed bucket, makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = currency_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/dim_currency/dim_currency_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/dim_currency/dim_currency_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/dim_currency/dim_currency_0003/test_file3.csv")

    result = currency_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/dim_currency/dim_currency_0001/test_file2.csv",
                            "Payment/dim_currency/dim_currency_0002/test_file.csv",
                            "Payment/dim_currency/dim_currency_0003/test_file3.csv"]



@mock_s3
def test_counterparty_files_processed_list(s3_client):
    """Counts number of dim_counterparty files in processed bucket, makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = counterparty_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/dim_counterparty/dim_counterparty_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/dim_counterparty/dim_counterparty_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/dim_counterparty/dim_counterparty_0003/test_file3.csv")

    result = counterparty_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/dim_counterparty/dim_counterparty_0001/test_file2.csv",
                            "Payment/dim_counterparty/dim_counterparty_0002/test_file.csv",
                            "Payment/dim_counterparty/dim_counterparty_0003/test_file3.csv"]




@mock_s3
def test_date_files_processed_list(s3_client):
    """Counts number of dim_date files in processed bucket, makes list
                        of their filenames and returns 0, [] when it's empty"""

    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket") 

        result = date_files_processed_list("my-processed-test-bucket")

        assert result[0] == 0
        assert result[1] == []


        s3_client.upload_file("test_file2.csv", "my-processed-test-bucket",
            "Payment/dim_date/dim_date_0001/test_file2.csv")
        s3_client.upload_file("test_file.csv", "my-processed-test-bucket",
            "Payment/dim_date/dim_date_0002/test_file.csv")
        s3_client.upload_file("test_file3.csv", "my-processed-test-bucket",
            "Payment/dim_date/dim_date_0003/test_file3.csv")

    result = date_files_processed_list("my-processed-test-bucket")

    assert result[0] == 3 
    assert result[1] == ["Payment/dim_date/dim_date_0001/test_file2.csv",
                            "Payment/dim_date/dim_date_0002/test_file.csv",
                            "Payment/dim_date/dim_date_0003/test_file3.csv"]





"""Testing uploading files to processed bucket"""
@mock_s3
def test_fact_payment_tables(s3_client):
    """Tests query run on payment dataframes is correct ensuring output
        fact_payment dataframes have correct columns and data and tests that
            function converts payment dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""

    file_text = """payment_id,created_at,last_updated,transaction_id,counterparty_id,payment_amount,currency_id,payment_type_id,paid,payment_date,company_ac_number,counterparty_ac_number
    2,2022-11-03 14:20:52.187,2022-11-03 14:20:52.187,2,15,552548.62,2,3,False,2022-11-04,67305075,31622269"""

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")
        s3_client.create_bucket(Bucket="my-processed-test-bucket")

        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment/test_file.csv")
        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/payment/test_file2.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/payment/test_file3.csv")

        df_list_payment_mock = df_list_payment(
                [
                "TableName/payment/test_file.csv", 
                "TableName/payment/test_file2.csv",
                "TableName/payment/test_file3.csv"
                ],
                s3=boto3.resource("s3"),
                s3_ingest_bucket_name="my-test-bucket")

    test_df = pd.DataFrame({
                            "payment_record_id": [1],
                            "payment_id": [2],
                            "created_date": ["2022-11-03"],
                            "created_time": ["14:20:52"],
                            "last_updated_date": ["2022-11-03"],
                            "last_updated_time": ["14:20:52"],
                            "transaction_id": [2],
                            "counterparty_id": [15],
                            "payment_amount": [552548.62],
                            "currency_id": [2],
                            "payment_type_id": [3],
                            "paid": [0],
                            "payment_date": ["2022-11-04"]
                            })

    payment_files_processed_list("my-processed-test-bucket")
        
    result = fact_payment_tables("my-processed-test-bucket",
        payment_files_processed_list("my-processed-test-bucket")[0],
        df_list_payment_mock)

    assert payment_files_processed_list("my-processed-test-bucket")[0] == 3
    assert payment_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/fact_payment/fact_payment_0001.parquet",
        "Payment/fact_payment/fact_payment_0002.parquet",
        "Payment/fact_payment/fact_payment_0003.parquet"]
    assert_frame_equal(result, test_df)



@mock_s3
def test_dim_payment_type_tables(s3_client):
    """Tests query run on payment_type dataframes is correct ensuring output
        dim_payment_type dataframes have correct columns and data and tests that
            function converts payment_type dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""

    file_text = """payment_type_id,payment_type_name,created_at,last_updated
    1,SALES_RECEIPT,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    2,SALES_REFUND,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    3,PURCHASE_PAYMENT,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    4,PURCHASE_REFUND,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")
        s3_client.create_bucket(Bucket="my-processed-test-bucket")

        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment_type/test_file.csv")
        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/payment_type/test_file2.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/payment_type/test_file3.csv")

        df_list_payment_type_mock = df_list_payment_type(
                [
                "TableName/payment_type/test_file.csv",
                "TableName/payment_type/test_file2.csv",
                "TableName/payment_type/test_file3.csv"
                ],
                s3=boto3.resource("s3"),
                s3_ingest_bucket_name="my-test-bucket")

    test_df = pd.DataFrame({
                            "payment_type_id": [1, 2, 3, 4],
                            "payment_type_name": ["SALES_RECEIPT", "SALES_REFUND", "PURCHASE_PAYMENT", "PURCHASE_REFUND"]
                            })

    result = dim_payment_type_tables("my-processed-test-bucket",
        payment_type_files_processed_list("my-processed-test-bucket")[0], 
        df_list_payment_type_mock)

    assert payment_type_files_processed_list("my-processed-test-bucket")[0] == 3
    assert payment_type_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/dim_payment_type/dim_payment_type_0001.parquet",
        "Payment/dim_payment_type/dim_payment_type_0002.parquet",
        "Payment/dim_payment_type/dim_payment_type_0003.parquet"]
    assert_frame_equal(result, test_df)



@mock_s3
def test_dim_transaction_tables(s3_client):
    """Tests query run on transaction dataframes is correct ensuring output
        dim_transaction dataframes have correct columns and data and tests that
            function converts transaction dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""

    file_text = """transaction_id,transaction_type,sales_order_id,purchase_order_id,created_at,last_updated
    1,PURCHASE,2.0,2.0,2022-11-03 14:20:52.186,2022-11-03 14:20:52.186
    2,PURCHASE,3.0,3.0,2022-11-03 14:20:52.187,2022-11-03 14:20:52.187
    3,SALE,1.0,1.0,2022-11-03 14:20:52.186,2022-11-03 14:20:52.186"""

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")
        s3_client.create_bucket(Bucket="my-processed-test-bucket")

        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/transaction/test_file.csv")
        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/transaction/test_file2.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/transaction/test_file3.csv")

        df_list_transaction_mock = df_list_transaction(
                [
                "TableName/transaction/test_file.csv", 
                "TableName/transaction/test_file2.csv",
                "TableName/transaction/test_file3.csv"
                ],
                s3=boto3.resource("s3"),
                s3_ingest_bucket_name="my-test-bucket")

    test_df = pd.DataFrame({
                            "transaction_id": [1, 2, 3],
                            "transaction_type": ["PURCHASE", "PURCHASE", "SALE"],
                            "sales_order_id": ["2", "3", "1"],
                            "purchase_order_id": ["2", "3", "1"]
                            })

    result = dim_transaction_tables("my-processed-test-bucket",
        transaction_files_processed_list("my-processed-test-bucket")[0],
        df_list_transaction_mock)

    assert transaction_files_processed_list("my-processed-test-bucket")[0] == 3
    assert transaction_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/dim_transaction/dim_transaction_0001.parquet",
        "Payment/dim_transaction/dim_transaction_0002.parquet",
        "Payment/dim_transaction/dim_transaction_0003.parquet"]
    assert_frame_equal(result, test_df)



@mock_s3
def test_dim_currency_tables(s3_client):
    """Tests query run on currency dataframes is correct ensuring output
        dim_currency dataframes have correct columns and data and tests that
            function converts currency dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""

    file_text = """currency_id,currency_code,created_at,last_updated
    1,GBP,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    2,USD,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962
    3,EUR,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")
        s3_client.create_bucket(Bucket="my-processed-test-bucket")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/currency/test_file.csv")
        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/currency/test_file2.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/currency/test_file3.csv")

        df_list_currency_mock = df_list_currency(
                [
                "TableName/currency/test_file.csv", 
                "TableName/currency/test_file2.csv",
                "TableName/currency/test_file3.csv"
                ],
                s3=boto3.resource("s3"),
                s3_ingest_bucket_name="my-test-bucket")

    test_df = pd.DataFrame({
                            "currency_id": [1, 2, 3],
                            "currency_code": ["GBP", "USD", "EUR"],
                            "currency_name": ["British Pound", "US Dollar", "Euro"]
                            })
        
    result = dim_currency_tables("my-processed-test-bucket",
        currency_files_processed_list("my-processed-test-bucket")[0],
        df_list_currency_mock)

    assert currency_files_processed_list("my-processed-test-bucket")[0] == 3
    assert currency_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/dim_currency/dim_currency_0001.parquet",
        "Payment/dim_currency/dim_currency_0002.parquet",
        "Payment/dim_currency/dim_currency_0003.parquet"]
    assert_frame_equal(result, test_df)




@mock_s3
def test_dim_counterparty_tables(s3_client):
    """Tests query run on counterparty dataframes is correct ensuring output
        dim_counterparty dataframes have correct columns and data and tests that
            function converts counterparty dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""

    file_text_counterparty = """counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated
    1,Fahey and Sons,15,Michael Toy,Mrs. Lucy Runolfsdottir,2022-11-03 14:20:51.563,2022-11-03 14:20:51.563"""

    file_text_address = """address_id,address_line_1,address_line_2,district,city,postal_code,country,phone,created_at,last_updated
    15,6826 Herzog Via, ,Avon,New Patienceburgh,28441,Turkey,1803 637401,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962"""

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_counterparty)
        with open("test_file2.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_counterparty)
        with open("test_file3.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_counterparty)

        with open("test_file4.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_address)
        with open("test_file5.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_address)
        with open("test_file6.csv", "w", encoding="UTF-8") as f:
            f.write(file_text_address)


    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket")

        s3_client.create_bucket(Bucket="my-processed-test-bucket")
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/counterparty/test_file.csv")
        s3_client.upload_file("test_file2.csv", "my-test-bucket",
            "TableName/counterparty/test_file2.csv")
        s3_client.upload_file("test_file3.csv", "my-test-bucket",
            "TableName/counterparty/test_file3.csv")

        s3_client.upload_file("test_file4.csv", "my-test-bucket",
            "TableName/address/test_file4.csv")
        s3_client.upload_file("test_file5.csv", "my-test-bucket",
            "TableName/address/test_file5.csv")
        s3_client.upload_file("test_file6.csv", "my-test-bucket",
            "TableName/address/test_file6.csv")

        df_list_counterparty_mock = df_list_counterparty(["TableName/counterparty/test_file.csv", 
                "TableName/counterparty/test_file2.csv",
                "TableName/counterparty/test_file3.csv"],
                s3=boto3.resource("s3"),s3_ingest_bucket_name="my-test-bucket")

        df_list_address_mock = df_list_address(["TableName/address/test_file4.csv", 
                "TableName/address/test_file5.csv",
                "TableName/address/test_file6.csv"],
                s3=boto3.resource("s3"),s3_ingest_bucket_name="my-test-bucket")

    test_df = pd.DataFrame({
                            "counterparty_id": [1],
                            "counterparty_legal_name": ["Fahey and Sons"],
                            "counterparty_legal_address_line_1": ["6826 Herzog Via"],
                            "counterparty_legal_address_line_2": [" "],
                            "counterparty_legal_district": ["Avon"],
                            "counterparty_legal_city": ["New Patienceburgh"],
                            "counterparty_legal_postal_code": [28441],
                            "counterparty_legal_country": ["Turkey"],
                            "counterparty_legal_phone_number": ["1803 637401"],
                            })    

        
    result = dim_counterparty_tables("my-processed-test-bucket",
        counterparty_files_processed_list("my-processed-test-bucket")[0],
        df_list_counterparty_mock, df_list_address_mock)


    assert counterparty_files_processed_list("my-processed-test-bucket")[0] == 3
    assert counterparty_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/dim_counterparty/dim_counterparty_0001.parquet",
        "Payment/dim_counterparty/dim_counterparty_0002.parquet",
        "Payment/dim_counterparty/dim_counterparty_0003.parquet"]
    assert_frame_equal(result, test_df)



@mock_s3
def test_dim_date_tables(s3_client):
    """Tests query run on date dataframes is correct ensuring output
        dim_date dataframes have correct columns and data and tests that
            function converts date dataframes to parquet and uploads them to
                processed bucket with correct filename and key"""


    file_text = "1, 2, 3,\n4, 5, 6,\n7, 8, 9"

    with NamedTemporaryFile(delete=True, suffix=".csv") as file:
        with open("test_file.csv", "w", encoding="UTF-8") as f:
            f.write(file_text)

    with mock_s3():
        s3_client.create_bucket(Bucket="my-test-bucket") 
        s3_client.upload_file("test_file.csv", "my-test-bucket",
            "TableName/payment/Run0001/test_file.csv")

    df_list_payment_mock= df_list_payment(payment_files_list("my-test-bucket"),
            s3= boto3.resource("s3"), s3_ingest_bucket_name = "my-test-bucket")


    with mock_s3():
        s3_client.create_bucket(Bucket="my-processed-test-bucket")

    test_df = pd.DataFrame({
                            "date_id": ["2022-11-03", "2022-11-04", "2022-11-05", "2022-11-06", "2022-11-07"],
                            "year": ["2022", "2022", "2022", "2022", "2022"],
                            "month": ["11", "11", "11", "11", "11"],
                            "day": ["03", "04", "05", "06", "07"],
                            "day_of_week": ["4", "5", "6", "7", "1"],
                            "day_name": ["Thursday", "Friday", "Saturday", "Sunday", "Monday"],
                            "month_name": ["November", "November", "November", "November", "November"],
                            "quarter": [4, 4, 4, 4, 4]
                            })

        
    result = dim_date_tables("my-processed-test-bucket",
        date_files_processed_list("my-processed-test-bucket")[0],
        df_list_payment_mock)

    assert date_files_processed_list("my-processed-test-bucket")[0] == 1
    assert date_files_processed_list("my-processed-test-bucket")[1] == [
        "Payment/dim_date/dim_date_0001.parquet"]
    assert_frame_equal(result.head(), test_df)