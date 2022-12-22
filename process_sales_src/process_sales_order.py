
from process_sales_order_utils import retrieve_ingested_csv, create_fact_sales_order_dataframe, create_staff_dim_table, create_currency_dim_table, create_design_dim_table, create_counterparty_dim_table, create_date_dim_table, get_run_number, save_to_processed_sales_bucket, save_to_processed_sales_bucket_as_csv, csvString_to_dict
import pandas as pd
from datetime import datetime as dt



run_num = get_run_number()

sales_order_data = retrieve_ingested_csv('sales_order', run_num)
staff_data = retrieve_ingested_csv('staff', run_num)
dept_data = retrieve_ingested_csv('department', run_num)
currency_data = retrieve_ingested_csv('currency', run_num)
des_data = retrieve_ingested_csv('design', run_num)
cp_data = retrieve_ingested_csv('counterparty', run_num)
ad_data = retrieve_ingested_csv('address', run_num)

# deal with commas inside quotes in counterparty data
temp_cp = [cp_row.split('"') for cp_row in cp_data.split('\n')]
for row in temp_cp:
    if len(row) > 1:
        row[1] = row[1].replace(',','<comma>')
    
cp_data = str(temp_cp)[2:len(str(temp_cp))-1].replace('], [', '\n').replace("', ","").replace("'","").replace('"','').replace(']','')


sales_order_dict = csvString_to_dict(sales_order_data)
dept_dict = csvString_to_dict(dept_data)
staff_dict = csvString_to_dict(staff_data)
currency_dict = csvString_to_dict(currency_data)
des_dict = csvString_to_dict(des_data)
cp_dict = csvString_to_dict(cp_data)
ad_dict = csvString_to_dict(ad_data)



sales_order_df = pd.DataFrame(sales_order_dict)
cp_sales_order_df = pd.DataFrame(sales_order_dict)
dept_df = pd.DataFrame(dept_dict)
staff_df = pd.DataFrame(staff_dict)
currency_df = pd.DataFrame(currency_dict)
design_df = pd.DataFrame(des_dict)
counterparty_df = pd.DataFrame(cp_dict)
address_df = pd.DataFrame(ad_dict)

fact_sales_order = create_fact_sales_order_dataframe(sales_order_df)
staff_dim = create_staff_dim_table(staff_df,dept_df)
currency_dim = create_currency_dim_table(currency_df)
design_dim = create_design_dim_table(design_df)
counterparty_dim = create_counterparty_dim_table(counterparty_df,address_df)
date_dim = create_date_dim_table(sales_order_df)

df_list = [fact_sales_order,staff_dim,currency_dim,design_dim,counterparty_dim,date_dim]
table_names = ['sales_order','staff','currency','design','counterparty','date']

for i in range(len(df_list)):
    save_to_processed_sales_bucket(table_names[i],run_num,df_list[i])








