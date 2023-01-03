from purchase_data_processing.src.retrieve_tables_ingested_bucket import retrieve_table_from_s3_bucket_convert_dataframe
from purchase_data_processing.src.dim_tables import create_staff_dim_dataframe, create_counterparty_dim_dataframe, create_currency_dim_dataframe, create_date_dim_dataframe, create_location_dim_dataframe
from purchase_data_processing.src.fact_purchase_order_table import create_fact_purchase_orders_dataframe
from purchase_data_processing.src.dataframe_to_parquet_upload_processed_bucket import convert_dataframe_to_parquet_and_upload_S3

def lambda_handler(event, context):
    ## Retrieving the tables from the ingested S3 bucket and converting into dataframes. 
    staff_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'staff')
    department_df = retrieve_table_from_s3_bucket_convert_dataframe("bosch-deploy-23-12-22-v2-ingest-bucket", 'department')
    counterparty_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'counterparty')
    address_df = retrieve_table_from_s3_bucket_convert_dataframe("bosch-deploy-23-12-22-v2-ingest-bucket", 'address')
    currency_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'currency')
    location_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'address')
    purchases_df = retrieve_table_from_s3_bucket_convert_dataframe('bosch-deploy-23-12-22-v2-ingest-bucket', 'purchase')

    # Creating the dim and fact tables in a dataframe
    counterparty_dim_df = create_counterparty_dim_dataframe(counterparty_df, address_df)
    staff_dim_df = create_staff_dim_dataframe(staff_df, department_df)
    currency_dim_df = create_currency_dim_dataframe(currency_df)
    location_dim_df = create_location_dim_dataframe(location_df)
    date_dim_df = create_date_dim_dataframe()
    purchases_fact_df = create_fact_purchase_orders_dataframe(purchases_df)

    # Converting the dim and fact table dataframe into parquet form and uploading directing to processed S3 bucket. 
    convert_dataframe_to_parquet_and_upload_S3(counterparty_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'counterparty_dim')
    convert_dataframe_to_parquet_and_upload_S3(staff_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'staff_dim')
    convert_dataframe_to_parquet_and_upload_S3(currency_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'currency_dim')
    convert_dataframe_to_parquet_and_upload_S3(location_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'location_dim')
    convert_dataframe_to_parquet_and_upload_S3(date_dim_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'date_dim')
    convert_dataframe_to_parquet_and_upload_S3(purchases_fact_df, 'bosch-deploy-23-12-22-v2-processed-bucket', 'purchase_fact')