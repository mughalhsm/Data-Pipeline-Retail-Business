import pg8000.native as pg
import boto3
import json
import pandas as pd
import logging


def retrieve_table_from_s3_bucket_in_dataframe(bucket_name, table_name):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name)
    buckets = []
    for object in response['Contents']:
        buckets.append(object['Key'])
    # Collects a list of this type of table in the S3 bucket by that name e.g staff tables
    buckets = [bucket for bucket in buckets if table_name != None and table_name in bucket]
    # Downloads updated version of that bucket
    s3.download_file(Bucket=bucket_name, Key=buckets[-1], Filename=f'{table_name}.csv')
    # Convert csv file into panda data frame. 
    return pd.read_csv(f'{table_name}.csv')


def convert_data_frame_to_parquet(dim_table_dataframe, name_of_parquet_file):
    dim_table_dataframe.to_parquet(f'{name_of_parquet_file}.parquet', engine='fastparquet')

def create_staff_dim_dataframe(staff_df, department_df):
    joined_df = pd.merge(left = staff_df, right = department_df, how = 'inner', on = 'department_id')
    new_df = joined_df.drop(['created_at_x', 'last_updated_x', 'manager', 'created_at_y', 'last_updated_y', 'last_updated_y'], axis=1)
    new_df = new_df.sort_values(by=['staff_id']).reset_index(drop=True)
    return new_df



def create_counterparty_dim_dataframe(counterparty_df, address_df):
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
    return new_df

def create_currency_dim_dataframe(currency_df):
    currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
    currency_dict = {'USD' : "US Dollars", 'GBP' : "Pound Sterling", 'EUR' : 'Euro'}
    currency_df['currency_name'] = currency_df['currency_code'].map(currency_dict)
    return currency_df

def create_location_dim_dataframe(location_df):
    location_df = location_df.drop(['created_at', 'last_updated'], axis=1)
    location_df.rename(columns = { "address_id" : "location_id"})
    return location_df


def create_date_dim_dataframe():
    df = pd.DataFrame(pd.date_range('1/1/2000','12/31/2025'), columns=['date_id'])
    df['year'] = df['date_id'].dt.year
    df['month'] = df['date_id'].dt.month
    df["day"] = df['date_id'].dt.day
    df["day_of_week"] = df['date_id'].dt.dayofweek
    df['day_name'] = df['date_id'].dt.strftime("%A")
    df['month_name'] = df['date_id'].dt.strftime("%B")
    df['quarter'] = df['date_id'].dt.quarter
    return df
