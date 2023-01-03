from purchase_data_processing.src.retrieve_tables_ingested_bucket import retrieve_table_from_s3_bucket_convert_dataframe
from purchase_data_processing.src.dim_tables import create_staff_dim_dataframe
from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3


def lambda_handler(event, context):
    staff_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'staff')
    department_df = retrieve_table_from_s3_bucket_convert_dataframe("bosch-deploy-23-12-22-v2-ingest-bucket", 'department')
    staff_dim_df = create_staff_dim_dataframe(staff_df, department_df)
    convert_dataframe_to_parquet_and_upload_S3(staff_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'staff_dim')

