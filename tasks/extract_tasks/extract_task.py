import luigi
from dotenv import load_dotenv
import os
import pandas as pd

from services.extract.extract import Extract
from utils.logger import logger_extract

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


logger = logger_extract



class ExtractSales(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logger.debug("▶ Starting extraction for Sales data")
        try:
            df_sales = extract_class.connection_to_database(url_source_database_sales, 'amazon_sales_data')
            logger.info("✅ Sales data successfully extracted")
        except ValueError as e:
            logger.warning(f"❌ Failed to extract Sales data (ValueError): {e}")
            return
        except Exception as e:
            logger.error(f"❌ Unexpected error while extracting Sales data: {e}")
            return
            
        logger.debug("💾 Saving Sales data to CSV")
        try:
            df_sales.to_csv(self.output()['sales'].path, index=False)
            logger.info("✅ Sales data successfully saved to CSV")
        except ValueError as e:
            logger.warning(f"❌ Failed to save Sales data to CSV (ValueError): {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error while saving Sales data to CSV: {e}")
    
    def output(self): 
        return { 'sales': luigi.LocalTarget("data/raw/extracted_sales.csv") }
    

class ExtractMarketing(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logger.debug("▶ Starting extraction for Marketing data")
        try:
            df_marketing = extract_class.extract_data_csv(path_file_sales)
            logger.info("✅ Marketing data successfully extracted")
        except ValueError as e:
            logger.warning(f"❌ Failed to extract Marketing data (ValueError): {e}")
            return
        except Exception as e:
            logger.error(f"❌ Unexpected error while extracting Marketing data: {e}")
            return
        
        logger.debug("💾 Saving Marketing data to CSV")
        try:
            df_marketing.to_csv(self.output()['marketing'].path, index=False)
            logger.info("✅ Marketing data successfully saved to CSV")
        except ValueError as e:
            logger.warning(f"❌ Failed to save Marketing data to CSV (ValueError): {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error while saving Marketing data to CSV: {e}")
    
    def output(self): 
        return { 'marketing': luigi.LocalTarget("data/raw/extracted_marketing.csv") }


class ExtractBooks(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        logger.debug("▶ Starting extraction for Books data")
        try:
            df_books = extract_class.extract_data_csv(path_file_book)
            logger.info("✅ Books data successfully extracted")
        except ValueError as e:
            logger.warning(f"❌ Failed to extract Books data (ValueError): {e}")
            return
        except Exception as e:
            logger.error(f"❌ Unexpected error while extracting Books data: {e}")
            return
        
        logger.debug("💾 Saving Books data to CSV")
        try:
            df_books.to_csv(self.output()['books'].path, index=False)
            logger.info("✅ Books data successfully saved to CSV")
        except ValueError as e:
            logger.warning(f"❌ Failed to save Books data to CSV (ValueError): {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error while saving Books data to CSV: {e}")
    
    def output(self): 
        return { 'books': luigi.LocalTarget("data/raw/extracted_books.csv") }
