#This file is invoked within ConnectionLambda to allow ingestion
import boto3, botocore
import pandas as pd
from botocore.exceptions import ClientError
import datetime
import time
import logging
s3 = boto3.resource('s3')
bucket_name = 'bosch-test-run-2-ingest-bucket'
prefix = 'Run-tracker'

def check_input_details_correct():
    print('Checking correct input of bucket_name and prefix...')
    if bucket_name != 'bosch-test-run-2-ingest-bucket' or prefix != 'Run-tracker':
        logging.error('Wrong names given')
        quit()
    else:
        return bucket_name, prefix



bucket = s3.Bucket(bucket_name)
def check_bucket(bucket):
    try:
        print('Checking if you have access to bucket...')
        s3.meta.client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist. 
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            logging.error("Private Bucket. Forbidden Access - Possible incorrect credentials")
            return True
        elif error_code == 404:
            logging.error("Bucket Does Not Exist!")
            return False, 'NEEDTOCREATE'




def create_initial_time_stamp_file(): ## only invoked if there is no template file 
    s3=boto3.client("s3")
    data = {
    "Run": [0],
    "Timestamp": ["2022-12-14 12:00:00"]
    }
    print('Creating intial time stamp file...')
    template_data = pd.DataFrame(data)
    template_data_csv=template_data.to_csv(index=False)
    s3.put_object(Bucket=bucket_name, Key="Run-tracker/run-number.csv", Body=template_data_csv)




def push_updated_file_back_to_bucket(dataframe): ## this function should overwrite
    s3client=boto3.client('s3') 
    dataframe_as_csv=dataframe.to_csv(index=False)
    s3client.put_object(Bucket=bucket_name, Key=f"Run-tracker/run-number{dataframe.at[0, 'Run']}.csv", Body=dataframe_as_csv)
    print('File pushed back up to bucket')
 



def increment_run_number(key_to_download, file_name):
    s3client=boto3.client('s3')
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M') 
    print('Downloading file...')
    try:

        s3client.download_file(Bucket=bucket_name,Key=key_to_download,Filename=f'/tmp/{file_name}')
    except ClientError as e:
        print(e.response['Error'])

    with open(f'/tmp/{file_name}', 'r') as f:

        print('Updating file...')
        df = pd.read_csv(f'/tmp/{file_name}') 
    try:
        df.at[0, 'Run'] +=1                 ## incrementing the run amount
        df.at[0, 'Timestamp'] = timestamp
        push_updated_file_back_to_bucket(df)
        UpdatedVal = df.at[0, 'Run']
        return UpdatedVal
    except KeyError as ke:
        print(ke, ' Most likely the template file is not correct')
        


def getting_last_object(bucket_name, prefix):
    print('Returning last object name for specific prefix...')
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    print(latest['Key'], '<- file name that will be incremented')

    last_file_key = latest['Key']
    parts = last_file_key.split("/")
    last_file_name = parts[1]
    UpdatedVal = increment_run_number(last_file_key, last_file_name)
    return UpdatedVal

def check_if_empty_bucket():
    print('checking if empty bucket')
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix) ## have to specify correctly, otherwise will be creating loads...
    except ClientError as e:
        logging.error('Could not list objects', e.response['Error'])
    return response['KeyCount']


def num_track_run_func():
    if check_if_empty_bucket() == 0:  
        print('Bucket is empty...')  
        create_initial_time_stamp_file()   
    else:
        print('Bucket is not empty') 

    if check_bucket(bucket)==True: ### This means i have access to the bucket, now we obtain last item which allows us to increment
        increment = getting_last_object(bucket_name, prefix)
        print(increment)
        return increment
    if increment == None:

        logging.error('Something is very wrong, check prefix and bucket_name are correct')
        quit()
    logging.info('Run number updated successfully, can continue to ingestion')
    return increment





# elif check_bucket(bucket)[1]=='NEEDTOCREATE':
#     print('Need to create that bucket')
    # create_initial_run_number_bucket(), --- going to assume deployment takes care of this bucket and then all that is needed is implementing a template file for 0 runs 

