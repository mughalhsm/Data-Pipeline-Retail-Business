import pg8000.native as pg
import boto3
import json
import pandas as pd
import logging
from botocore.exceptions import ClientError



def retrieve_table_from_s3_bucket_convert_dataframe(bucket_name, table_name):
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
        s3.download_file(Bucket=bucket_name, Key=latest['Key'], Filename=f'table_files/{table_name}.csv')
    except ClientError as error:
        raise error
    except Exception as e:
        raise e
    else:
        return pd.read_csv(f'table_files/{table_name}.csv')

x = retrieve_table_from_s3_bucket_convert_dataframe('cees-nc-test-bucket-2', 'design')
print(x)


def create_staff_dim_dataframe(staff_df, department_df):
    try:
        joined_df = pd.merge(left = staff_df, right = department_df, how = 'inner', on = 'department_id')
        new_df = joined_df.drop(['created_at_x', 'last_updated_x', 'manager', 'created_at_y', 'last_updated_y', 'last_updated_y'], axis=1)
        new_df = new_df.sort_values(by=['staff_id']).reset_index(drop=True)
    except Exception as e:
        raise e
    else:
        return new_df

def convert_data_frame_to_parquet(dim_table_dataframe, name_of_parquet_file):
    dim_table_dataframe.to_parquet(f'{name_of_parquet_file}.parquet', engine='fastparquet')




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
    except Exception as e:
        raise e
    else: 
        return new_df

def create_currency_dim_dataframe(currency_df):
    try:
        currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
        currency_dict = {'USD' : "US Dollars", 'GBP' : "Pound Sterling", 'EUR' : 'Euro'}
        currency_df['currency_name'] = currency_df['currency_code'].map(currency_dict)
    except Exception as e:
        raise e
    else:
        return currency_df

def create_location_dim_dataframe(location_df):
    try:
        location_df = location_df.drop(['created_at', 'last_updated'], axis=1)
        location_df.rename(columns = { "address_id" : "location_id"})
    except Exception as e:
        raise e
    else:
        return location_df


def create_date_dim_dataframe():
    try:
        df = pd.DataFrame(pd.date_range('1/1/2000','12/31/2025'), columns=['date_id'])
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



