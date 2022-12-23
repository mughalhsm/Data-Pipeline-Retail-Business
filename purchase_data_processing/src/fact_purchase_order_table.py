import pandas as pd

def create_fact_purchase_orders_dataframe(purchase_order_df):
    try:
        if "purchase_order_id" not in purchase_order_df.columns:
            raise KeyError
        purchase_order_df['created_date'] = pd.to_datetime(purchase_order_df['created_at']).dt.date
        purchase_order_df['created_time'] = pd.to_datetime(purchase_order_df['created_at']).dt.time
        purchase_order_df['last_updated_date'] = pd.to_datetime(purchase_order_df['last_updated']).dt.date
        purchase_order_df['last_updated_time'] = pd.to_datetime(purchase_order_df['last_updated']).dt.time
        purchase_order_df = purchase_order_df.drop(['created_at', 'last_updated'], axis=1)
        purchase_order_df['purchase_record_id'] = ''
    except Exception as e:
        print(e)
        raise e
    else:
        return purchase_order_df
