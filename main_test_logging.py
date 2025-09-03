import luigi
from dotenv import load_dotenv
import os
import pandas as pd
import logging

from ETL.extract.extract import Extract
from ETL.transform.transform import Transform
from ETL.load.load import Load

load_dotenv()

extract_class = Extract()
transform_class = Transform()
load_class = Load()

# Env variables
username_sales = os.getenv('SOURCE_POSTGRES_USER')
password_sales = os.getenv('SOURCE_POSTGRES_PASSWORD')
host_sales = os.getenv('SOURCE_POSTGRES_HOST')
port_sales = os.getenv('SOURCE_POSTGRES_PORT',)
db_sales = os.getenv('SOURCE_POSTGRES_DB')

# Marketing file
path_file_sales = os.getenv('PATH_MARKETING')

# DB URL
url_source_database_sales = f'postgresql://{username_sales}:{password_sales}@{host_sales}:{port_sales}/{db_sales}'

# DB BOOK
path_file_book = os.getenv('PATH_BOOKS')

logger = logging.getLogger("luigi-interface")

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    filename="logs/app.log",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)



logging.debug("----------------------------ETL Started-----------------------------------------")

class ExtractData(luigi.Task):
    def requires(self):
        pass
    
    def run(self):
        #extract data sales, marketing, boooks
        
        logging.debug('----------------------------------Start Extracting Data Sales--------------------------------------------------------')
        try:
            df_sales = extract_class.connection_to_database(url_source_database_sales,'amazon_sales_data')
            logging.info('✅ Data sales berhasil di extract')
        except ValueError:
            logging.warning('❌ Value on extracting error')
        except Exception:
            logging.error('❌ Error pada proses extract')

        logging.debug('----------------------------------Start Extracting Data Marketing--------------------------------------------------------')
        try:
            df_marketing = extract_class.extract_data_csv(path_file_sales)
            logging.info('✅ Data marketing berhasil di extract')
        except ValueError:
            logging.warning('❌ Value on extracting error')
        except Exception:
            logging.error('❌ Error pada proses extract')
        
        logging.debug('----------------------------------Start Extracting Data Books--------------------------------------------------------')
        try:
            df_books = extract_class.extract_data_csv(path_file_book)
            logging.info('✅ Data books berhasil di extract')
        except ValueError:
            logging.warning('❌ Value on extracting error')
        except Exception:
            logging.error('❌ Error pada proses extract')

        logging.debug('-------------------------------------Start extract Data to CSV (Sales)--------------------------------------------------')
        #extract to csv
        try:
            df_sales.to_csv(self.output()['sales'].path, index=False)
            logging.info("✅ Data Sales Successfully Extracted")
        except ValueError as e:
            logging.warning(f'❌ Error value extracting sales data: {e}')
        except Exception as e:
            logging.error(f"❌ Error extracting sales data: {e}")

        logging.debug('-------------------------------------Start extract Data to CSV (Marketing)--------------------------------------------------')
        try:
            df_marketing.to_csv(self.output()['marketing'].path, index=False)
            logging.info("✅ Data Marketing Successfully Extracted")
        except ValueError as e:
            logging.warning(f'❌ Error value extracting sales data: {e}')
        except Exception as e:
            logging.error(f"❌ Error extracting marketing data: {e}")
        
        logging.debug('-------------------------------------Start extract Data to CSV (Books)--------------------------------------------------')
        try:
            df_books.to_csv(self.output()['books'].path, index=False)
            logging.info("✅ Data Books Successfully Extracted")
        except ValueError as e:
            logging.warning(f'❌ Error value extracting sales data: {e}')
        except Exception as e:
            logging.error(f"❌ Error extracting books data: {e}")
    
    def output(self): 
        return { 'sales': luigi.LocalTarget("data/raw/extracted_sales.csv"), 
                'marketing': luigi.LocalTarget("data/raw/extracted_marketing.csv"),
                'books': luigi.LocalTarget("data/raw/extracted_books.csv")}


class TransformData(luigi.Task):
    def requires(self):
        return ExtractData()
    
    def run(self):
        input_files = self.input()
        
        # extracting data sales
        logger.debug("--------------------------------Transforming data Sales-------------------------------------------")
        try:
            df_sales = pd.read_csv(input_files['sales'].path)
            logger.info('✅ get data from source successfuly')
        except ValueError:
            logger.warning(f'❌ Error extracting marketing data: {e}')
        except Exception:
            logger.error(f'❌ Error extracting marketing data: {e}')

        # check data null on sales
        logger.debug('Check Data Null on df_sales')
        logger.info(transform_class.check_null(df_sales))
        # drop data null dibawah 1%
        logger.debug('Drop data null on sales data')
        try:    
            df_sales_drop = transform_class.drop_null_data(df_sales)
            logger.info('✅ null data berhasil di drop dari sales data berhasil ')
        except Exception:
            logger.error('❌ gagal drop data null pada sales data')
       
        #drop unknow colums
        logger.info('Drop Column yang tidak diketahui')
        df_sales_drop = transform_class.drop_columns_null(df_sales_drop, 'Unnamed: 0')
        # change discount price to float
        df_sales_drop['discount_price'] = df_sales_drop['discount_price'].apply(transform_class.replace_row_sales)
        df_sales_drop['actual_price'] = df_sales_drop['actual_price'].apply(transform_class.replace_row_sales)
        df_sales_drop['no_of_ratings'] = df_sales_drop['no_of_ratings'].apply(transform_class.replace_row_sales).astype(int)
        df_sales_drop['ratings'] = df_sales_drop['ratings'].apply(transform_class.replace_row_sales).astype(int)
        df_sales_clean = df_sales_drop
        # check data null after Transform
        print("================================================Check data Null After Transform=================================================================")
        print(transform_class.check_null(df_sales_clean))
        print()
        print()
        print()
        
        
        # extracting data marketing --> need to fix
        print("--------------------------------Transforming data Marketing-------------------------------------------")
        df_sales = pd.read_csv(input_files['marketing'].path)
        print('================================================Check Data Null =================================================================================')
        #check null data
        print(transform_class.check_null(df_marketing)) # check null data
        df_marketing_null = transform_class.check_null(df_marketing)

        print('Check Data Duplicate ====================================================================================================')

        #check duplicate data
        print(transform_class.check_duplicated(df_marketing)) # check duplicated data

        print('Transform Marketing data ====================================================================================================')
        #transform data marketing

        # get colums has null higher than 60%
        null_columns = transform_class.get_colums_null_list(df_marketing_null)
        # drop columns null higher than 60%
        df_dropped_null = transform_class.drop_columns_null(df_marketing, null_columns)
        print(f'Columns {null_columns} has been dropped')

        #Start Transform marketing data
        df_dropped_null['prices.shipping'] = df_dropped_null.apply(
            transform_class.fill_shipping,
            axis=1,
            args=('prices.shipping', 'prices.amountMin')  
        )
        print("start transform Price_shipping---------------------------------------------------------")

        df_dropped_null['manufacturer'] = df_dropped_null.apply(transform_class.fill_null, axis=1)

        print("start transform manufacturer------------------------------------------------------------")

        df_dropped_null['shipping_cost'] = df_dropped_null['prices.shipping'].apply(transform_class.create_shipping_to_float)

        print("start create shipping_cost----------------------------------------------------------------")

        df_marketing_clean = transform_class.drop_null_data(df_dropped_null)

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
        df_marketing_clean = transform_class.rename_col(df_marketing_clean,cols_new)
        

        # check duplicate and null
        print(transform_class.check_duplicated(df_marketing_clean))
        print(transform_class.check_null(df_marketing_clean))
        print()
        print()
        print()
        
        
        # extracting data books
        print("--------------------------------Transforming data books-------------------------------------------")
        data_book_scrap = pd.read_csv(input_files['books'].path)
        
        print(transform_class.check_duplicated(data_book_scrap))
        print(transform_class.check_null(data_book_scrap))

        # Transform price col
        data_book_scrap['price'] = data_book_scrap['price'].apply(transform_class.replace_row).astype(float)

        # Trasform rating col
        data_book_scrap['rating'] = data_book_scrap['rating'].apply(transform_class.rating_to_int).astype(int)
        
        print('Transform data Finished ====================================================================================================')
                
                
        #extract to csv
        try:
            df_sales_clean.to_csv(self.output()['sales'].path, index=False)
            print("✅ Data Sales Successfully Extracted")
        except Exception as e:
            print(f"❌ Error extracting sales data: {e}")
            
        try:
            df_marketing_clean.to_csv(self.output()['marketing'].path, index=False)
            print("✅ Data Sales Successfully Extracted")
        except Exception as e:
            print(f"❌ Error extracting marketing data: {e}")
            
        try:
            data_book_scrap.to_csv(self.output()['books'].path, index=False)
            print("✅ Data Sales Successfully Extracted")
        except Exception as e:
            print(f"❌ Error extracting books data: {e}")
            
        
        
    def output(self): 
        return { 'sales': luigi.LocalTarget("data/transform/extracted_sales.csv"),
                'marketing': luigi.LocalTarget("data/transform/extracted_marketing.csv"),
                'books': luigi.LocalTarget("data/transform/extracted_books.csv")}










if __name__ == "__main__":
    luigi.build([ExtractData()], local_scheduler=True)
