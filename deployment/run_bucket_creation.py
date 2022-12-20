from deployment.src.create_buckets import Create_resources

testing_prefix = "bosch-deploy-2-"



def create_buckets():
    ingest_lambda_path = "Ingestion/src"
    process_payments_lambda_path = ""
    process_purchases_lambda_path = "purchase_data_processing/src"
    process_sales_lambda_path = ""
    warehouse_uploader_lambda_path = ""
    code_bucket_name = f'{testing_prefix}code-bucket'
    processed_bucket_name = f'{testing_prefix}processed-bucket'
    ingest_bucket_name = f'{testing_prefix}ingest-bucket'
    create = Create_resources()
    create.create_s3_bucket(code_bucket_name)
    create.create_s3_bucket(processed_bucket_name)
    create.create_s3_bucket(ingest_bucket_name)
    ingest_lambda_name = f"{testing_prefix}ingest"
    process_payments_lambda_name = f"{testing_prefix}process_payments"
    process_purchases_lambda_name = f"{testing_prefix}process_purchases"
    process_sales_lambda_name = f"{testing_prefix}process_sales"
    upload_lambda_name = f"{testing_prefix}upload"
    if ingest_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=ingest_lambda_path, lambda_name=ingest_lambda_name)
    if process_payments_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_payments_lambda_path,
                                           lambda_name=process_payments_lambda_name)
    if process_purchases_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_purchases_lambda_path,
                                           lambda_name=process_purchases_lambda_name)
    if process_sales_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_sales_lambda_path,
                                           lambda_name=process_sales_lambda_name)
    if warehouse_uploader_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=warehouse_uploader_lambda_path, lambda_name=upload_lambda_name)


if __name__ == '__main__':
    create_buckets()
