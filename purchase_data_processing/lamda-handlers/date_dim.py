from purchase_data_processing.src.dim_tables import create_date_dim_dataframe
from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3

def lambda_handler(event, context):
    date_dim_df = create_date_dim_dataframe()
    convert_dataframe_to_parquet_and_upload_S3(date_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'date_dim')
