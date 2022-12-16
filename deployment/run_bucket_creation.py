from deployment.src.create_buckets import Create_resources


def create_buckets():
    ingest_lambda_path = ""
    process_payments_lambda_path = ""
    process_purchases_lambda_path = ""
    process_sales_lambda_path = ""
    warehouse_uploader_lambda_path = ""
    code_bucket_name = 'code-bucket'
    processed_bucket_name = 'processed-bucket'
    ingest_bucket_name = 'ingest-bucket'
    create = Create_resources()
    create.create_s3_bucket('code-bucket')
    create.create_s3_bucket('ingest-bucket')
    create.create_s3_bucket('processed-bucket')
    ingest_lambda_name = "ingest"
    process_payments_lambda_name = "process_payments"
    process_purchases_lambda_name = "process_purchases"
    process_sales_lambda_name = "process_sales"
    upload_lambda_name = "upload"
    if ingest_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=ingest_lambda_path, lambda_name=ingest_lambda_name)
    if process_payments_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_payments_lambda_path,
                                           lambda_name=process_payments_lambda_name, pandas_dependency=True)
    if process_purchases_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_purchases_lambda_path,
                                           lambda_name=process_purchases_lambda_name, pandas_dependency=True)
    if process_sales_lambda_path != "":
        create.upload_lambda_function_code(code_bucket=code_bucket_name, folder_path=process_sales_lambda_path,
                                           lambda_name=process_sales_lambda_name, pandas_dependency=True)
    if warehouse_uploader_lambda_path != "":
        create.upload_lambda_function_code(
            code_bucket=code_bucket_name, folder_path=warehouse_uploader_lambda_path, lambda_name=upload_lambda_name)


if __name__ == '__main__':
    create_buckets()
