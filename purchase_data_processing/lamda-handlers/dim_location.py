from purchase_data_processing.src.retrieve_tables_ingested_bucket import retrieve_table_from_s3_bucket_convert_dataframe
from purchase_data_processing.src.dim_tables import create_location_dim_dataframe
from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3

def lambda_handler(event, context):
    location_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'address')
    location_dim_df = create_location_dim_dataframe(location_df)
    convert_dataframe_to_parquet_and_upload_S3(location_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'location_dim')
