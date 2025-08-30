from ETL.extract.extract import Extract
from ETL.transform.transform import Transform

extract_class = Extract()
Transform_class = Transform()

# scrapping

BASE_TEMPLATE = "https://books.toscrape.com/catalogue/page-{}.html"
BASE_SITE = "https://books.toscrape.com/catalogue/"
BASE_IMAGE = "https://books.toscrape.com/"
data_book_scrap = extract_class.scrap_book_data(50,BASE_TEMPLATE,BASE_SITE,BASE_IMAGE)

print(data_book_scrap)

# extract database

url = 'postgresql://postgres:password123@localhost:5432/etl_db'
print(extract_class.connection_to_database(url,'amazon_sales_data'))
df_sales = extract_class.connection_to_database(url,'amazon_sales_data')

# extract csv
path_file = 'Data_source/ElectronicsProductsPricingData.csv'
print(extract_class.extract_data_csv(path_file))
df_marketing = extract_class.extract_data_csv(path_file)



# Transform data

print('Check Data Null ====================================================================================================')

#check null data
print(Transform_class.check_null(df_marketing)) # check null data
df_marketing_null = Transform_class.check_null(df_marketing)
print(Transform_class.check_null(df_sales)) # check null data sales

print('Check Data Duplicate ====================================================================================================')

#check duplicate data
print(Transform_class.check_duplicated(df_marketing)) # check duplicated data
print(Transform_class.check_duplicated(df_sales)) # check duplicated data sales

print('Transform Marketing data ====================================================================================================')
#transform data marketing

# get colums has null higher than 60%
null_columns = Transform_class.get_colums_null_list(df_marketing_null)
# drop columns null higher than 60%
df_dropped_null = Transform_class.drop_columns_null(df_marketing, null_columns)
print(f'Columns {null_columns} has been dropped')

#Start Transform marketing data
df_dropped_null['prices.shipping'] = df_dropped_null.apply(
    Transform_class.fill_shipping,
    axis=1,
    args=('prices.shipping', 'prices.amountMin')  
)

print("start transform Price_shipping---------------------------------------------------------")

df_dropped_null['manufacturer'] = df_dropped_null.apply(Transform_class.fill_null, axis=1)

print("start transform manufacturer------------------------------------------------------------")

df_dropped_null['shipping_cost'] = df_dropped_null['prices.shipping'].apply(Transform_class.create_shipping_to_float)

print("start create shipping_cost----------------------------------------------------------------")

df_marketing_clean = Transform_class.drop_null_data(df_dropped_null)

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
df_marketing_clean = Transform_class.rename_col(df_marketing_clean,cols_new)

# check duplicate and null
print(Transform_class.check_duplicated(df_marketing_clean))
print(Transform_class.check_null(df_marketing_clean))

print('Transform Marketing data Finished ====================================================================================================')

# Transform sales data
print(df_sales)

print("Transform data Duplicated=================================================================================================================")
# check data duplicated
jumlah_data = len(df_sales)
nilai = Transform_class.check_duplicated(df_sales)
print(nilai + f' dari {jumlah_data}')
print(Transform_class.check_null(df_sales))

# drop data null dibawah 1%
df_drop_null = Transform_class.drop_null_data(df_sales)
# drop duplicated data
df_drop_duplicate = Transform_class.drop_duplicate(df_drop_null)


# check data duplicated and null
print(Transform_class.check_duplicated(df_drop_duplicate))
print(Transform_class.check_null(df_drop_duplicate))

df_sales_clean = df_drop_duplicate

# Transform data hasil scrap
print(Transform_class.check_duplicated(data_book_scrap))
print(Transform_class.check_null(data_book_scrap))

# Transform price col
data_book_scrap['price'] = data_book_scrap['price'].apply(Transform_class.replace_row).astype(float)

# Trasform rating col
data_book_scrap['rating'] = data_book_scrap['rating'].apply(Transform_class.rating_to_int).astype(int)

print(data_book_scrap)


# LOAD TO DATABASE