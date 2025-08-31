from dotenv import load_dotenv
import os
load_dotenv()
from ETL.extract.extract import Extract
from ETL.transform.transform import Transform
from ETL.load.load import Load
extract_class = Extract()
transfrom_class = Transform()
load_class = Load()

BASE_TEMPLATE = "https://books.toscrape.com/catalogue/page-{}.html"
BASE_SITE = "https://books.toscrape.com/catalogue/"
BASE_IMAGE = "https://books.toscrape.com/"
data_book_scrap = extract_class.scrap_book_data(50,BASE_TEMPLATE,BASE_SITE,BASE_IMAGE)

print(data_book_scrap)

# Transform data hasil scrap
print(transfrom_class.check_duplicated(data_book_scrap))
print(transfrom_class.check_null(data_book_scrap))

# Transform price col
data_book_scrap['price'] = data_book_scrap['price'].apply(transfrom_class.replace_row).astype(float)

# Trasform rating col
data_book_scrap['rating'] = data_book_scrap['rating'].apply(transfrom_class.rating_to_int).astype(int)

print(data_book_scrap)

username = os.getenv('ETL_POSTGRES_USER')
password = os.getenv('ETL_POSTGRES_PASSWORD')
host = os.getenv('ETL_HOST')
port = os.getenv('ETL_PORT')
database = os.getenv('ETL_POSTGRES_DB')

conn = load_class.get_conn_from_database(username, password, host, port, database)

print(conn.connect())

print("Load Data to Database".ljust(70, "=") + ">")
load_class.load_data_to_db(data_book_scrap, "data_book", conn)