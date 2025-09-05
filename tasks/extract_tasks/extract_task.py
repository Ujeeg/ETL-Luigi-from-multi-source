import luigi
from dotenv import load_dotenv
import os
import pandas as pd
import logging

from services.extract.extract import Extract

# Load .env 
load_dotenv()

extract_class = Extract()

# Env variables for database sales
username_sales = os.getenv('SOURCE_POSTGRES_USER')
password_sales = os.getenv('SOURCE_POSTGRES_PASSWORD')
host_sales = os.getenv('SOURCE_POSTGRES_HOST')
port_sales = os.getenv('SOURCE_POSTGRES_PORT',)
db_sales = os.getenv('SOURCE_POSTGRES_DB')

# DB connection string
url_source_database_sales = f'postgresql://{username_sales}:{password_sales}@{host_sales}:{port_sales}/{db_sales}'

# File paths
path_file_sales = os.getenv('PATH_MARKETING')
path_file_book = os.getenv('PATH_BOOKS')

# Create logs folder if not exist
os.makedirs("logs", exist_ok=True)

# Config logger 
logging.basicConfig(
    level=logging.DEBUG,  
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/extract.log", mode="a"),  
        logging.StreamHandler()  
    ]
)
logger = logging.getLogger("Extract-Data")


class ExtractSales(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logging.debug("▶ Starting extraction for Sales data")
        try:
            df_sales = extract_class.connection_to_database(url_source_database_sales, 'amazon_sales_data')
            logging.info("✅ Sales data successfully extracted")
        except ValueError as e:
            logging.warning(f"❌ Failed to extract Sales data (ValueError): {e}")
            return
        except Exception as e:
            logging.error(f"❌ Unexpected error while extracting Sales data: {e}")
            return
            
        logging.debug("💾 Saving Sales data to CSV")
        try:
            df_sales.to_csv(self.output()['sales'].path, index=False)
            logging.info("✅ Sales data successfully saved to CSV")
        except ValueError as e:
            logging.warning(f"❌ Failed to save Sales data to CSV (ValueError): {e}")
        except Exception as e:
            logging.error(f"❌ Unexpected error while saving Sales data to CSV: {e}")
    
    def output(self): 
        return { 'sales': luigi.LocalTarget("data/raw/extracted_sales.csv") }
    

class ExtractMarketing(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logging.debug("▶ Starting extraction for Marketing data")
        try:
            df_marketing = extract_class.extract_data_csv(path_file_sales)
            logging.info("✅ Marketing data successfully extracted")
        except ValueError as e:
            logging.warning(f"❌ Failed to extract Marketing data (ValueError): {e}")
            return
        except Exception as e:
            logging.error(f"❌ Unexpected error while extracting Marketing data: {e}")
            return
        
        logging.debug("💾 Saving Marketing data to CSV")
        try:
            df_marketing.to_csv(self.output()['marketing'].path, index=False)
            logging.info("✅ Marketing data successfully saved to CSV")
        except ValueError as e:
            logging.warning(f"❌ Failed to save Marketing data to CSV (ValueError): {e}")
        except Exception as e:
            logging.error(f"❌ Unexpected error while saving Marketing data to CSV: {e}")
    
    def output(self): 
        return { 'marketing': luigi.LocalTarget("data/raw/extracted_marketing.csv") }


class ExtractBooks(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logging.debug("▶ Starting extraction for Books data")
        try:
            df_books = extract_class.extract_data_csv(path_file_book)
            logging.info("✅ Books data successfully extracted")
        except ValueError as e:
            logging.warning(f"❌ Failed to extract Books data (ValueError): {e}")
            return
        except Exception as e:
            logging.error(f"❌ Unexpected error while extracting Books data: {e}")
            return
        
        logging.debug("💾 Saving Books data to CSV")
        try:
            df_books.to_csv(self.output()['books'].path, index=False)
            logging.info("✅ Books data successfully saved to CSV")
        except ValueError as e:
            logging.warning(f"❌ Failed to save Books data to CSV (ValueError): {e}")
        except Exception as e:
            logging.error(f"❌ Unexpected error while saving Books data to CSV: {e}")
    
    def output(self): 
        return { 'books': luigi.LocalTarget("data/raw/extracted_books.csv") }
