import pg8000.native as pg
import boto3
import json
import pandas as pd
import logging
from botocore.exceptions import ClientError
from io import StringIO, BytesIO



def retrieve_table_from_s3_bucket_convert_dataframe(bucket_name, table_name):
    if table_name == 'purchase order' or table_name == 'purchases' or table_name == 'purchase':
        table_name = 'purchase_order'
    try:
        s3 = boto3.client('s3')
        paginator = s3.get_paginator( "list_objects_v2" )
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=f'TableName/{table_name}/')
        latest = None
        ## Finds the most updated version of this table by iterating through all the pages 
        ## of this S3 bucket and find "Last Modified" for required 'table_name'.
        for page in page_iterator:
            if "Contents" in page:
                latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
                if latest is None or latest2['LastModified'] > latest['LastModified']:
                    latest = latest2
        if latest == None:
            raise ValueError
        ## Reads the content of table required
        s3_object = s3.get_object(Bucket=bucket_name, Key=latest['Key'])
        csv_string = s3_object['Body'].read().decode('utf-8')
        ## Converts string into a format that can be used by pandas to convert into dataframe
        csv_df_input = StringIO(csv_string)
        ## Converts CSV string (StringIO) into dataframe.
        table_as_df = pd.read_csv(csv_df_input, sep=',')
    except ClientError as error:
        raise error
    except ValueError as error:
        print(f" '{table_name}' table does NOT exist in this bucket - please check bucket or table name.")
        raise error
    except Exception as e:
        raise e
    else:
        return table_as_df


def create_staff_dim_dataframe(staff_df, department_df):
    try:
        joined_df = pd.merge(left = staff_df, right = department_df, how = 'inner', on = 'department_id')
        new_df = joined_df.drop(['created_at_x', 'last_updated_x', 'manager', 'created_at_y', 'last_updated_y', 'last_updated_y', 'department_id'], axis=1)
        new_df = new_df.sort_values(by=['staff_id']).reset_index(drop=True)
        new_df = new_df.loc[:,['staff_id','first_name','last_name', 'department_name', 'location', 'email_address']]
    except Exception as e:
        raise e
    else:
        return new_df

def create_counterparty_dim_dataframe(counterparty_df, address_df):
    try:
        joined_df = pd.merge(left = counterparty_df, right = address_df, how = 'inner', left_on = 'legal_address_id', right_on = 'address_id')
        new_df = joined_df.drop(['delivery_contact', 'commercial_contact', 'created_at_x', 'last_updated_x', 'created_at_y', 'last_updated_y', 'legal_address_id'], axis=1)
        new_df = new_df.sort_values(by=['counterparty_id']).reset_index(drop=True)
        new_df.rename(columns = {
            'address_id':'counterparty_address_id', 
            'address_line_1':'counterparty_address_line_1', 
            "address_line_2" : "counterparty_address_line_2", 
            'district' : "counterparty_district",
            'city' : "counterparty_city",
            'postal_code' : "counterparty_postal_code",
            'country' : "counterparty_country",
            'phone' : "counterparty_phone"
            }, inplace = True)
        print(new_df.columns)
    except KeyError as e:
        print("Incorrect dataframe as arguement or order of arguements incorrect - please check")
        raise e
    except TypeError as e:
        print('Incorrect type of arguement - arguements must be dataframes.')
        raise e
    except Exception as e:
        raise e
    else: 
        return new_df


def create_currency_dim_dataframe(currency_df):
    try:
        currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
        currency_dict = {'USD' : "US Dollars", 'GBP' : "Pound Sterling", 'EUR' : 'Euro'}
        currency_df['currency_name'] = currency_df['currency_code'].map(currency_dict)
    except KeyError as e:
        print("Incorrect dataframe as arguement or order of arguements incorrect - please check")
        raise e
    except AttributeError as e:
        print('Incorrect type of arguement - arguements must be dataframes.')
        raise e
    except Exception as e:
        raise e
    else:
        return currency_df

def create_location_dim_dataframe(address_df):
    try:
        if "address_id" not in address_df.columns:
            raise KeyError
        address_df = address_df.drop(['created_at', 'last_updated'], axis=1)
        address_df.rename(columns = { "address_id" : "location_id"})
    except Exception as e:
        raise e
    else:
        return address_df

def create_date_dim_dataframe():
    try:
        df = pd.DataFrame(pd.date_range('1/1/2020','12/31/2025'), columns=['date_id'])
        df['year'] = df['date_id'].dt.year
        df['month'] = df['date_id'].dt.month
        df["day"] = df['date_id'].dt.day
        df["day_of_week"] = df['date_id'].dt.dayofweek
        df['day_name'] = df['date_id'].dt.strftime("%A")
        df['month_name'] = df['date_id'].dt.strftime("%B")
        df['quarter'] = df['date_id'].dt.quarter
    except Exception as e:
        raise e
    else:
        return df



def create_fact_purchase_orders_dataframe(purchase_order_df):
    try:
        if "purchase_order_id" not in purchase_order_df.columns:
            raise KeyError
        purchase_order_df['created_date'] = pd.to_datetime(purchase_order_df['created_at']).dt.date
        purchase_order_df['created_time'] = pd.to_datetime(purchase_order_df['created_at']).dt.time
        purchase_order_df['last_updated_date'] = pd.to_datetime(purchase_order_df['last_updated']).dt.date
        purchase_order_df['last_updated_time'] = pd.to_datetime(purchase_order_df['last_updated']).dt.time
        purchase_order_df = purchase_order_df.drop(['created_at', 'last_updated'], axis=1)
    except Exception as e:
        print(e)
        raise e
    else:
        return purchase_order_df



def convert_dataframe_to_parquet_and_upload_S3(table_dataframe, bucket_name, filename):
    out_buffer = BytesIO()
    table_dataframe.to_parquet(out_buffer)
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=f"{filename}.parquet", Body=out_buffer.getvalue())

# purchase_order_df = pd.read_csv('../test/table_files/purchase_order.csv')
# fact = create_fact_purchase_orders_dataframe(purchase_order_df)
# print(fact.columns)