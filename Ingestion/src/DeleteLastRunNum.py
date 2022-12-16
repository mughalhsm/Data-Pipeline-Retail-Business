import boto3

def delete_last_run_num_object(bucket_name, prefix):
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
    print(latest['Key'], '<- Removing...')
    s3.delete_object(Bucket=bucket_name, Key=latest['Key'])
   
