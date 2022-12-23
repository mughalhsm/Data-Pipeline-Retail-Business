from deployment.src.create_buckets import Create_resources

deploy_prefix = "bosch-deploy-23-12-22-v2-"



def create_buckets():
    ingest_lambda_path = "Ingestion/src"
    process_payments_lambda_path = ""
    process_purchases_lambda_path = "purchase_data_processing/src"
    process_sales_lambda_path = "process_sales_src"
    warehouse_uploader_lambda_path = "Uploading/"
    code_bucket_name = f'{deploy_prefix}code-bucket'
    processed_bucket_name = f'{deploy_prefix}processed-bucket'
    ingest_bucket_name = f'{deploy_prefix}ingest-bucket'
    
    print("Creating buckets")
    create = Create_resources()
    response = create.create_s3_bucket(code_bucket_name)
    print(f'Created code bucket with response {response}')
    response = create.create_s3_bucket(processed_bucket_name)
    print(f'Created processed bucket with response {response}')
    response = create.create_s3_bucket(ingest_bucket_name)
    print(f'Created ingest bucket with response {response}')
    
    ingest_lambda_name = f"{deploy_prefix}ingest"
    process_payments_lambda_name = f"{deploy_prefix}process_payments"
    process_purchases_lambda_name = f"{deploy_prefix}process_purchases"
    process_sales_lambda_name = f"{deploy_prefix}process_sales"
    upload_lambda_name = f"{deploy_prefix}upload"
    
    ingest_prerequisite = "pg8000.zip"
    process_prerequisite = "fastpandas.zip"
    output_prerequisite = "pg8000.zip" #"psychopg2.zip"
    
    print('uploading lambdas where valid')
    if ingest_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=ingest_lambda_path, lambda_name=ingest_lambda_name,prerequisite_zip=ingest_prerequisite)
    if process_payments_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_payments_lambda_path,
                                           lambda_name=process_payments_lambda_name,prerequisite_zip=process_prerequisite)
    if process_purchases_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_purchases_lambda_path,
                                           lambda_name=process_purchases_lambda_name,prerequisite_zip=process_prerequisite)
    if process_sales_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_sales_lambda_path,
                                           lambda_name=process_sales_lambda_name,prerequisite_zip=process_prerequisite)
    if warehouse_uploader_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=warehouse_uploader_lambda_path, lambda_name=upload_lambda_name,prerequisite_zip=output_prerequisite)


if __name__ == '__main__':
    create_buckets()
