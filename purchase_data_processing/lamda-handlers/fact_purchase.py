from purchase_data_processing.src.retrieve_tables_ingested_bucket import retrieve_table_from_s3_bucket_convert_dataframe
from purchase_data_processing.src.fact_purchase_order_table import create_fact_purchase_orders_dataframe
from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3

def lambda_handler(event, context):
    purchases_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'purchase')
    purchases_fact_df = create_fact_purchase_orders_dataframe(purchases_df)
    convert_dataframe_to_parquet_and_upload_S3(purchases_fact_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'purchase_fact')
