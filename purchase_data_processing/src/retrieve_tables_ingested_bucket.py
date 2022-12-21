import pg8000.native as pg
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from io import StringIO



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

