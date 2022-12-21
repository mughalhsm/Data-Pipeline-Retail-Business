import boto3
from io import BytesIO


def convert_dataframe_to_parquet_and_upload_S3(table_dataframe, bucket_name, filename):
    out_buffer = BytesIO()
    table_dataframe.to_parquet(out_buffer)
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=f"{filename}.parquet", Body=out_buffer.getvalue())