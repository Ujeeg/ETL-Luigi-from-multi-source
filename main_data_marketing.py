from dotenv import load_dotenv
import os
load_dotenv()
from ETL.extract.extract import Extract
from ETL.transform.transform import Transform
from ETL.load.load import Load
extract_class = Extract()
transfrom_class = Transform()
load_class = Load()

# extract csv
path_file = 'Data_source/ElectronicsProductsPricingData.csv'
print(extract_class.extract_data_csv(path_file))
df_marketing = extract_class.extract_data_csv(path_file)

# Transform data

print('Check Data Null ====================================================================================================')

#check null data
print(transfrom_class.check_null(df_marketing)) # check null data
df_marketing_null = transfrom_class.check_null(df_marketing)

print('Check Data Duplicate ====================================================================================================')

#check duplicate data
print(transfrom_class.check_duplicated(df_marketing)) # check duplicated data

print('Transform Marketing data ====================================================================================================')
#transform data marketing

# get colums has null higher than 60%
null_columns = transfrom_class.get_colums_null_list(df_marketing_null)
# drop columns null higher than 60%
df_dropped_null = transfrom_class.drop_columns_null(df_marketing, null_columns)
print(f'Columns {null_columns} has been dropped')

#Start Transform marketing data
df_dropped_null['prices.shipping'] = df_dropped_null.apply(
    transfrom_class.fill_shipping,
    axis=1,
    args=('prices.shipping', 'prices.amountMin')  
)

print("start transform Price_shipping---------------------------------------------------------")

df_dropped_null['manufacturer'] = df_dropped_null.apply(transfrom_class.fill_null, axis=1)

print("start transform manufacturer------------------------------------------------------------")

df_dropped_null['shipping_cost'] = df_dropped_null['prices.shipping'].apply(transfrom_class.create_shipping_to_float)

print("start create shipping_cost----------------------------------------------------------------")

df_marketing_clean = transfrom_class.drop_null_data(df_dropped_null)

#rename columns
cols_new = {
    'prices.amountMax': 'prices_amount_max',
    'prices.amountMin': 'prices_amount_min',
    'prices.availability': 'prices_availability',
    'prices.condition': 'prices_condition',
    'prices.currency': 'prices_currency',
    'prices.dateSeen': 'prices_date_seen',
    'prices.isSale': 'prices_is_sale',
    'prices.merchant': 'prices_merchant',
    'prices.shipping': 'prices_shipping',
    'prices.sourceURLs': 'prices_source_urls',
    'manufacturerNumber': 'manufacturer_number',
    'primaryCategories': 'primary_categories',
    'imageURLs': 'image_urls',
    'sourceURLs': 'source_urls',
    'shipping_cost': 'shipping_cost'
}
df_marketing_clean = transfrom_class.rename_col(df_marketing_clean,cols_new)

# check duplicate and null
print(transfrom_class.check_duplicated(df_marketing_clean))
print(transfrom_class.check_null(df_marketing_clean))

print('Transform Marketing data Finished ====================================================================================================')


print(df_marketing_clean)
print(df_marketing_clean.dtypes)

df_marketing_clean.to_csv(r"test_new_transform\products.csv", index=False)

username = os.getenv('ETL_POSTGRES_USER')
password = os.getenv('ETL_POSTGRES_PASSWORD')
host = os.getenv('ETL_HOST')
port = os.getenv('ETL_PORT')
database = os.getenv('ETL_POSTGRES_DB')

#conn = load_class.get_conn_from_database(username, password, host, port, database)
#print("Load Data to Database".ljust(70, "=") + ">")
#load_class.load_data_to_db(df_marketing_clean, "data_marketing", conn)