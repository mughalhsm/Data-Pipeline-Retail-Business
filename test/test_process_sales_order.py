import pytest
from unittest.mock import patch
from moto import mock_s3
import boto3
from process_sales_src.process_sales_order_utils import retrieve_ingested_csv, get_run_number, create_fact_sales_order_dataframe, create_staff_dim_table, create_currency_dim_table,create_counterparty_dim_table, create_design_dim_table,create_date_dim_table,save_to_processed_sales_bucket,create_location_dim_table
from io import StringIO, BytesIO
import pandas as pd
import datetime

@mock_s3
def test_get_run_num_correctly_gets_largest_run_num():

    test_file = StringIO()
    test_file.write('test')
    conn = boto3.client('s3')

    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')

    for i in range(11):
        conn.put_object(Body=test_file.getvalue(), Bucket='bosch-deploy-23-12-22-v2-ingest-bucket', Key=f'Run-tracker/run-number{i}.csv')
    
    result = get_run_number()
    print(conn.list_objects_v2(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket', Prefix='Run-tracker'))
    assert result == 10

@mock_s3
def test_get_run_num_raises_error_if_no_runs_found():

    
    conn = boto3.client('s3')

    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')

    with pytest.raises(Exception):
        get_run_number()




@mock_s3
def test_retrieve_ingested_correctly_retrieves_csv_data():

    test_df = pd.DataFrame({'first_name':['Ben','Scott', 'Omar', 'Hamza','Cees'],'surname':['Ainger','Florence','Gonga','Mughal','Dear'],'team_name':['LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch']})
    test_csv = StringIO()
    test_df.to_csv(test_csv)

    conn = boto3.client('s3')

    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')
    conn.put_object(Body=test_csv.getvalue(),Bucket='bosch-deploy-23-12-22-v2-ingest-bucket',Key='TableName/address/RunNum:4.csv')
    result = retrieve_ingested_csv('address', 4)

    
    expected = test_csv.getvalue()

    assert result == expected

@mock_s3
def test_retrieve_ingested_correctly_returns_LookupError_if_given_invalid_table_name():

    test_df = pd.DataFrame({'first_name':['Ben','Scott', 'Omar', 'Hamza','Cees'],'surname':['Ainger','Florence','Gonga','Mughal','Dear'],'team_name':['LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch']})
    test_csv = StringIO()
    test_df.to_csv(test_csv)

    conn = boto3.client('s3')

    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')
    conn.put_object(Body=test_csv.getvalue(),Bucket='bosch-deploy-23-12-22-v2-ingest-bucket',Key='TableName/address/RunNum:4.csv')
    with pytest.raises(LookupError):
        retrieve_ingested_csv('not_a_table',6)

@mock_s3
def test_retrieve_ingested_correctly_returns_LookupError_if_given_invalid_run_number():

    test_df = pd.DataFrame({'first_name':['Ben','Scott', 'Omar', 'Hamza','Cees'],'surname':['Ainger','Florence','Gonga','Mughal','Dear'],'team_name':['LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch','LikeaBosch']})
    test_csv = StringIO()
    test_df.to_csv(test_csv)

    conn = boto3.client('s3')

    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-ingest-bucket')
    conn.put_object(Body=test_csv.getvalue(),Bucket='bosch-deploy-23-12-22-v2-ingest-bucket',Key='TableName/address/RunNum:4.csv')
    with pytest.raises(LookupError):
        retrieve_ingested_csv('address','not a number')
 
def test_create_fact_sales_order_dataframe():

    expected_headers = ['sales_order_id', 'design_id', 'staff_id', 'counterparty_id', 'units_sold',
 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date',
 'agreed_delivery_location_id', 'created_date', 'created_time',
 'last_updated_date', 'last_updated_time']

    test_headers = ['sales_order_id','created_at','last_updated','design_id','staff_id','counterparty_id','units_sold','unit_price','currency_id','agreed_delivery_date','agreed_payment_date','agreed_delivery_location_id']

    test_rows = [[1,'2022-11-03 14:20:52.186','2022-11-03 14:20:52.186',9,16,18,84754,2.43,3,'2022-11-10','2022-11-03',4],
[2,'2022-11-03 14:20:52.186','2022-11-03 14:20:52.186',3,19,8,42972,3.94,2,'2022-11-07','2022-11-08',8]]

    expected_row_1 = [2, 3, 19, 8, 42972, 3.94, 2, datetime.date(2022, 11, 7),
 datetime.date(2022, 11, 8), 8, datetime.date(2022, 11, 3),
 datetime.time(14, 20, 52, 186000), datetime.date(2022, 11, 3),
 datetime.time(14, 20, 52, 186000)]

    test_dict = {}

    for i in range(len(test_headers)):
        
        test_dict[test_headers[i]] = [test_rows[0][i],test_rows[1][i]]
    
    test_df = pd.DataFrame(test_dict)

    result_df = create_fact_sales_order_dataframe(test_df)

    assert result_df.columns.values.tolist() == expected_headers
    assert result_df.iloc[1].values.tolist() == expected_row_1

def test_create_fact_sales_order_raises_key_error_if_passed_unexpected_dataframe():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_fact_sales_order_dataframe(test_error_df)

def test_create_staff_dim_table():

    test_headers_1 = ['staff_id','first_name','last_name','email_address','department_id']
    test_headers_2 = ['department_id', 'department_name', 'location']
    expected_headers = ['staff_id','first_name', 'last_name', 'email_address', 'department_name', 'location']


    test_rows_1 = [[1,'Ben','Ainger','test@test.com',1],
[2,'Not Ben','Not Ainger','Secondtest@test.com',1]]

    test_dept_df = pd.DataFrame({'department_id':[1], 'department_name':['testers'], 'location':['remote']})


    expected_row_1 = [1,'Ben','Ainger','test@test.com','testers','remote']
    expected_row_2 = [2,'Not Ben','Not Ainger','Secondtest@test.com','testers','remote']
    test_dict = {}

    for i in range(len(test_headers_1)):
        
        test_dict[test_headers_1[i]] = [test_rows_1[0][i],test_rows_1[1][i]]
    
    test_df = pd.DataFrame(test_dict)

    result_df = create_staff_dim_table(test_df,test_dept_df)

    assert result_df.columns.values.tolist() == expected_headers
    assert result_df.iloc[0].values.tolist() == expected_row_1
    assert result_df.iloc[1].values.tolist() == expected_row_2
    
def test_create_staff_dim_raises_key_error_if_passed_unexpected_dataframe():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})
    test_error_df_2 = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_staff_dim_table(test_error_df,test_error_df_2)

def test_create_currency_dim_table():

    test_dict = {'currency_id':[1,2,3], 'currency_code':['GBP','USD','EUR'], 'created_at':[1,2,3], 'last_updated':[1,2,3]}

    expected_headers = ['currency_id', 'currency_code','currency_name']
    expected_rows = [[1,'GBP','Great British Pound'],[2,'USD','US Dollar'],[3,'EUR','Euro']]
    
    test_df = pd.DataFrame(test_dict)

    result_df = create_currency_dim_table(test_df)

    assert result_df.columns.values.tolist() == expected_headers
    for i in range(3):
        assert result_df.iloc[i].values.tolist() == expected_rows[i]
    
def test_create_currency_dim_table_raises_KeyError_if_passed_incorrect_DF():

    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})
   
    with pytest.raises(KeyError):
        create_currency_dim_table(test_error_df)

def test_create_couterparty_dim_table():

    test_counterparty_headers = ['counterparty_id','counterparty_legal_name','legal_address_id','commercial_contact','delivery_contact','created_at','last_updated']
    test_counterparty_rows = [[1,'Fahey and Sons',15,'Micheal Toy','Mrs. Lucy Runolfsdottir','2022-11-03 14:20:51.563','2022-11-03 14:20:51.563'],
                            [2,"Leannon, Predovic and Morar",28,'Melba Sanford','Jean Hane III','2022-11-03 14:20:51.563','2022-11-03 14:20:51.563']]
    test_counterparty_dict = {}

    test_address_headers = ['address_id','address_line_1','address_line_2','district','city','postal_code','country','phone','created_at','last_updated']
    test_address_rows = [[15,'605 Haskell Trafficway','Axel Freeway','','East Bobbie','88253-4257','Heard Island and McDonald Islands','9687 937447','2022-11-03 14:20:49.962','2022-11-03 14:20:49.962'],
                        [28,'079 Horacio Landing','','','Utica','93045','Austria','7772 084705','2022-11-03 14:20:49.962','2022-11-03 14:20:49.962']]
    test_address_dict = {}

    expected_headers = ['counterparty_id','counterparty_legal_name','counterparty_legal_address_line_1','counterparty_legal_address_line_2','counterparty_legal_district','counterparty_legal_city','counterparty_legal_postal_code','counterparty_legal_country','counterparty_legal_phone_number']
    expected_rows = [[1, 'Fahey and Sons', '605 Haskell Trafficway', 'Axel Freeway', '',
'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands',
  '9687 937447'], [2, 'Leannon, Predovic and Morar', '079 Horacio Landing', '', '', 'Utica',
  '93045', 'Austria', '7772 084705']]
    for i in range(len(test_counterparty_headers)):
        test_counterparty_dict[test_counterparty_headers[i]] = [test_counterparty_rows[0][i], test_counterparty_rows[1][i]]
    
    for i in range(len(test_address_headers)):
        test_address_dict[test_address_headers[i]] = [test_address_rows[0][i], test_address_rows[1][i]]
    test_counterparty_df = pd.DataFrame(test_counterparty_dict)
    test_address_df = pd.DataFrame(test_address_dict)

    result_df = create_counterparty_dim_table(test_counterparty_df,test_address_df)

    assert result_df.columns.values.tolist() == expected_headers
    for i in range(2):
        assert result_df.iloc[i].values.tolist() == expected_rows[i]
    
def test_create_counterparty_dim_raises_key_error_if_passed_unexpected_dataframe():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})
    test_error_df_2 = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_counterparty_dim_table(test_error_df,test_error_df_2)


def test_create_design_dim_table():

    test_headers = ['design_id', 'created_at', 'design_name', 'file_location', 'file_name',
                    'last_updated']

    test_rows = [['1', '2022-11-03 14:20:49.962', 'Wooden', '/home/user/dir',
                 'wooden-20201128-jdvi.json', '2022-11-03 14:20:49.962'],
                ['3', '2022-11-03 14:20:49.962', 'Steel', '/usr/ports',
                'steel-20210621-13gb.json', '2022-11-03 14:20:49.962']]

    test_dict = {}

    for i in range(len(test_headers)):
        test_dict[test_headers[i]] = [test_rows[0][i], test_rows[1][i]]

    test_df = pd.DataFrame(test_dict)

    expected_headers = ['design_id', 'design_name', 'file_location', 'file_name']
    expected_rows = [[1, 'Wooden', '/home/user/dir', 'wooden-20201128-jdvi.json'],
                     [3, 'Steel', '/usr/ports', 'steel-20210621-13gb.json']]

    result = create_design_dim_table(test_df)

    assert result.columns.values.tolist() == expected_headers
    for i in range(2):
        assert result.iloc[i].values.tolist() == expected_rows[i]
    

def test_create_design_dim_raises_key_error_if_passed_unexpected_dataframe():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_design_dim_table(test_error_df)

def test_date_dim_table():

    test_headers = ['sales_order_id','created_at','last_updated','design_id','staff_id','counterparty_id','units_sold','unit_price','currency_id','agreed_delivery_date','agreed_payment_date','agreed_delivery_location_id']

    test_rows = [1,'2022-11-03 14:20:52.186','2022-11-04 14:20:52.186',9,16,18,84754,2.43,3,'2022-11-10','2022-11-09',4]

    test_dict = {}

    for i in range(len(test_headers)):
        test_dict[test_headers[i]] = [test_rows[i]]

    test_df_sales = pd.DataFrame(test_dict)

    expected_headers = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    expected_rows = [[datetime.date(2022, 11, 3), 2022, 11, 3, 3, 'Thursday', 'November', 4],
                    [datetime.date(2022, 11, 4), 2022, 11, 4, 4, 'Friday', 'November', 4],
                    [datetime.date(2022, 11, 9), 2022, 11, 9, 2, 'Wednesday', 'November', 4],
                    [datetime.date(2022, 11, 10), 2022, 11, 10, 3, 'Thursday', 'November', 4]]
    
    result = create_date_dim_table(test_df_sales)
    assert result.columns.values.tolist() == expected_headers
    for i in range(4):
        assert result.iloc[i].values.tolist() in expected_rows
   

def test_date_dim_table_does_not_duplicate_dates():

    test_headers = ['sales_order_id','created_at','last_updated','design_id','staff_id','counterparty_id','units_sold','unit_price','currency_id','agreed_delivery_date','agreed_payment_date','agreed_delivery_location_id']

    test_rows = [1,'2022-11-03 14:20:52.186','2022-11-03 14:20:52.186',9,16,18,84754,2.43,3,'2022-11-10','2022-11-10',4]

    test_dict = {}

    for i in range(len(test_headers)):
        test_dict[test_headers[i]] = [test_rows[i]]

    test_df_sales = pd.DataFrame(test_dict)

    result = create_date_dim_table(test_df_sales)

    assert result['date_id'].values.tolist() == list(set(result['date_id'].values.tolist()))

def test_create_date_dim_raises_key_error_if_passed_unexpected_dataframe():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_date_dim_table(test_error_df)


def test_create_location_dim_table():

    test_headers = ['address_id','address_line_1','address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated']
    test_rows = [[1,'test house','test lane','North Testchester', 'Testchester', 'TE5 5ST','Testland', '01234567890','time', 'later time'], [2, 'test cottage','test avenue','South Testchester', 'Testchester', 'TS5 5ET', 'Testland', '09876543210', 'time', 'later time']]

    expected_headers = ['location_id','address_line_1','address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    expected_rows = [[1,'test house','test lane','North Testchester', 'Testchester', 'TE5 5ST', 'Testland', '01234567890'], [2, 'test cottage','test avenue','South Testchester', 'Testchester', 'TS5 5ET', 'Testland', '09876543210']]

    test_dict = {}
    print(len(test_headers))
    print(len(test_rows[0]))
    print(len(test_rows[1]))
    for i in range(len(test_headers)):
        test_dict[test_headers[i]] = [test_rows[0][i], test_rows[1][i]]

    test_df = pd.DataFrame(test_dict)

    result = create_location_dim_table(test_df)

    assert result.columns.values.tolist() == expected_headers
    for i in range(2):
        assert result.iloc[i].values.tolist() == expected_rows[i]
    
def test_create_location_dim_table_raises_key_error_if_passed_dataframe_invalid():
    
    test_error_df = pd.DataFrame({'Wrong Key':['random','data']})

    with pytest.raises(KeyError):
        create_location_dim_table(test_error_df)


@mock_s3
def test_save_to_processed_sales_bucket():

    test_df = pd.DataFrame({'test_key':[1,2,3], 'test_key_2':[3,4,5]})
    
    conn = boto3.client('s3')
    conn.create_bucket(Bucket='bosch-deploy-23-12-22-v2-processed-bucket')

    save_to_processed_sales_bucket('location', 11, test_df)

    key = conn.list_objects_v2(Bucket='bosch-deploy-23-12-22-v2-processed-bucket')['Contents'][0]['Key']

    expected_key = 'Sales/location/RunNum:11.parquet'

    result_data = BytesIO()

    conn.download_fileobj('bosch-deploy-23-12-22-v2-processed-bucket', 'Sales/location/RunNum:11.parquet', result_data)

    uploaded_df = pd.read_parquet(result_data)

    expected_columns = ['test_key', 'test_key_2']
    expected_rows = [[1,3], [2,4], [3,5]]

    assert key == expected_key
    assert uploaded_df.columns.values.tolist() == expected_columns
    for i in range(3):
        assert uploaded_df.iloc[i].values.tolist() == expected_rows[i]

@mock_s3  
def test_save_to_processed_sales_bucket_raises_TypeError_if_passed_RunNum_is_not_an_int():

    test_df = pd.DataFrame({'test_key':[1,2,3], 'test_key_2':[3,4,5]})

    with pytest.raises(TypeError):
        save_to_processed_sales_bucket('location','NaN', test_df)

@mock_s3  
def test_save_to_processed_sales_bucket_raises_LookupError_if_passed_invalid_table_name():

    test_df = pd.DataFrame({'test_key':[1,2,3], 'test_key_2':[3,4,5]})

    with pytest.raises(LookupError):
        save_to_processed_sales_bucket('random name',11, test_df)


