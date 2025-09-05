from dotenv import load_dotenv
import os
load_dotenv()
from services.extract.extract import Extract
from services.transform.transform import Transform
from services.load.load import Load
extract_class = Extract()
transfrom_class = Transform()
load_class = Load()

BASE_TEMPLATE = "https://books.toscrape.com/catalogue/page-{}.html"
BASE_SITE = "https://books.toscrape.com/catalogue/"
BASE_IMAGE = "https://books.toscrape.com/"
data_book_scrap = extract_class.scrap_book_data(50,BASE_TEMPLATE,BASE_SITE,BASE_IMAGE)

print(data_book_scrap)

# Transform data hasil scrap
print(data_book_scrap)

output_dir = "Data_source"
os.makedirs(output_dir, exist_ok=True)  # Pastikan folder ada

output_path = os.path.join(output_dir, "books_data.csv")
data_book_scrap.to_csv(output_path, index=False)