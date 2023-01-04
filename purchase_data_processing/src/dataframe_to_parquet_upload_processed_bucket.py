import boto3
from io import BytesIO


def convert_dataframe_to_parquet_and_upload_S3(table_dataframe, bucket_name, filename):
    ## Using BytesIO to allow capture of text without downlloading file locally.
    out_buffer = BytesIO()
    
    ## Converting dataframe into parquet form.
    table_dataframe.to_parquet(out_buffer)
    
    ## Uploading parquet file to processed bucket in S3. 
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=f"{filename}.parquet", Body=out_buffer.getvalue())