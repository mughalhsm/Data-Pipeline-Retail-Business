from process_sales_order import process_sales_schema
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def sales_handler(event, context):
    

    try: 
        run_num = process_sales_schema()

        logger.info(f'Processing of Sales Schema - Run Number {run_num} - successfully completed')

    except Exception as e:
        logger.error(f'{e}')
        
        




