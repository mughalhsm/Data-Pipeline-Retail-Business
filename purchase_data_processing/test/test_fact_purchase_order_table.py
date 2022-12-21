from purchase_data_processing.src.fact_purchase_order_table import create_fact_purchase_orders_dataframe
import pandas as pd

def test_create_fact_purchase_order_table_correctly():
    purchase_order_df = pd.read_csv('test_table_files/purchase_order.csv')
    fact_purchase_order_df = create_fact_purchase_orders_dataframe(purchase_order_df)
    column_names = fact_purchase_order_df.columns.tolist()
    expected_column_names = ['purchase_order_id', 'staff_id', 'counterparty_id', 'item_code',
       'item_quantity', 'item_unit_price', 'currency_id',
       'agreed_delivery_date', 'agreed_payment_date',
       'agreed_delivery_location_id', 'created_date', 'created_time',
       'last_updated_date', 'last_updated_time']
    assert column_names == expected_column_names
    assert len(fact_purchase_order_df.index) > 1
