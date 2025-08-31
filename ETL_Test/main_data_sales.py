from dotenv import load_dotenv
import os
load_dotenv()
from ETL.extract.extract import Extract
from ETL.transform.transform import Transform
from ETL.load.load import Load

extract_class = Extract()
transfrom_class = Transform()
load_class = Load()


url = 'postgresql://postgres:password123@localhost:5432/etl_db'
df_sales = extract_class.connection_to_database(url,'amazon_sales_data')

# Transform sales data
print('======================================================Data Sales==========================================================================')
print(df_sales)
print('==========================================================================================================================================')
print()
print("================================================Check data Null=================================================================")
print(transfrom_class.check_null(df_sales))
print()

# drop data null dibawah 1%
df_sales_drop = transfrom_class.drop_null_data(df_sales)
#drop unknow colums
df_sales_drop = transfrom_class.drop_columns_null(df_sales_drop, 'Unnamed: 0')
# change discount price to float
df_sales_drop['discount_price'] = df_sales_drop['discount_price'].apply(transfrom_class.replace_row_sales)
df_sales_drop['actual_price'] = df_sales_drop['actual_price'].apply(transfrom_class.replace_row_sales)
df_sales_drop['no_of_ratings'] = df_sales_drop['no_of_ratings'].apply(transfrom_class.replace_row_sales).astype(int)
df_sales_drop['ratings'] = df_sales_drop['ratings'].apply(transfrom_class.replace_row_sales).astype(int)
df_sales_clean = df_sales_drop.reset_index()


print()
# check data duplicated and null
print("================================================Check data Null After Transform=================================================================")
print(transfrom_class.check_null(df_sales_drop))
print()
print()
print("================================================Data After Transform=================================================================")
print(df_sales_drop)
print()


username = os.getenv('ETL_POSTGRES_USER')
password = os.getenv('ETL_POSTGRES_PASSWORD')
host = os.getenv('ETL_HOST')
port = os.getenv('ETL_PORT')
database = os.getenv('ETL_POSTGRES_DB')

conn = load_class.get_conn_from_database(username, password, host, port, database)

print(conn.connect())

load_class.load_data_to_db(df_sales_drop, "data_sales", conn)
