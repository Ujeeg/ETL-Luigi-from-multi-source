from tasks.extract_tasks.extract_task import ExtractBooks, ExtractMarketing, ExtractSales
from services.transform.transform import Transform
import luigi
import pandas as pd
import logging
import os

transform_class = Transform()

logger = logging.getLogger("Transform-Data")

logging.basicConfig(
    level=logging.DEBUG,  
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/transform.log", mode="a"),  
        logging.StreamHandler()  
    ]
)

os.makedirs("transform", exist_ok=True)


# transform data sales class
class TransformDataSales(luigi.Task):
    

    def requires(self):
        return ExtractSales()
    
    def run(self):
        input_files = self.input()
        
        # extracting data sales
        logger.debug("--------------------------------Transforming data Sales-------------------------------------------")
        try:
            df_sales = pd.read_csv(input_files['sales'].path)
        except ValueError:
            logger.warning('error value')
        except Exception:
            logger.error('Errorrr')
            
        # check data null on sales
        logger.debug('-------------------------------Data Null on df_sales------------------------------------------')
        logger.info(transform_class.check_null(df_sales))
        
        # drop data null dibawah 1%
        try:
            df_sales_drop = transform_class.drop_null_data(df_sales)
        except ValueError:
            logger.warning('error value')
        except Exception:
            logger.error('Errorrr')
        
        #drop unknow colums
        try:
            df_sales_drop = transform_class.drop_columns_null(df_sales_drop, 'Unnamed: 0')
        except ValueError:
            logger.warning('error value')
        except Exception:
            logger.error('Errorrr')
        
        # change discount price to float
        try:
            df_sales_drop['discount_price'] = df_sales_drop['discount_price'].apply(transform_class.replace_row_sales)
            df_sales_drop['actual_price'] = df_sales_drop['actual_price'].apply(transform_class.replace_row_sales)
            df_sales_drop['no_of_ratings'] = df_sales_drop['no_of_ratings'].apply(transform_class.replace_row_sales).astype(int)
            df_sales_drop['ratings'] = df_sales_drop['ratings'].apply(transform_class.replace_row_sales).astype(int)
            df_sales_clean = df_sales_drop
        except ValueError:
            logger.warning('error value')
        except Exception:
            logger.error('Errorrr')
            
            
        # check data null after Transform
        logger.debug("================================================Check data Null After Transform=================================================================")
        logger.info(transform_class.check_null(df_sales_clean))
        
        try:
            df_sales_clean.to_csv(self.output()['sales'].path, index=False)
            logger.info("✅ Data Sales Successfully Extracted")
        except Exception as e:
            logger.warning(f"❌ Error extracting sales data: {e}")
        except ValueError as e:
            logger.warning(f"❌ Error extracting sales data: {e}")
    
    def output(self):
        return { 'sales': luigi.LocalTarget("data/transform/extracted_sales.csv") }






class TransformDataMarketng(luigi.Task):
    
    def requires(self):
        return ExtractMarketing()

    def run(self):
        input_files = self.input()
        
        
        # extracting data marketing --> need to fix
        logger.debug("Transforming data Marketing")
        logger.debug('Read data to dataframe')
        
        try:
            df_marketing = pd.read_csv(input_files['marketing'].path)
            logger.info('File read successfully')
        except ValueError:
            logger.warning('Value Error')
        except Exception:
            logger.error('Error on reader func')
        
        
        logger.debug('Validate Data')  
        # check data null
        logger.debug('Check Data Null')
        try:
            logger.info(transform_class.check_null(df_marketing))
            df_marketing_null = transform_class.check_null(df_marketing)
        except ValueError:
            logger.warning('Value Error')
        except Exception:
            logger.error('Errorr')
            
        # check duplicates Data
        logger.debug('Check Data Duplicate')
        try:
            logger.info(transform_class.check_duplicated(df_marketing)) # check duplicated d
        except ValueError:
            logger.warning('value error')
        except Exception:
            logger.error('Error')
            

        #transform data marketin
        logger.debug('Get higher null data columns then 60%')
        # get higher than 60%
        try:
            null_columns = transform_class.get_colums_null_list(df_marketing_null)
        # drop columns null higher than 60%
            df_dropped_null = transform_class.drop_columns_null(df_marketing, null_columns)
            logger.info(f'Columns {null_columns} has been dropped')
        except ValueError:
            logger.warning('value Error')
        except Exception:
            logger.error('Error')

        logger.debug('Start Transforming Data')
        
        #Start Transform marketing data
        logger.debug('Start handling data Null on prices.shipping')
        try:
            df_dropped_null['prices.shipping'] = df_dropped_null.apply(
                transform_class.fill_shipping,
                axis=1,
                args=('prices.shipping', 'prices.amountMin')  
            )
            logger.info('price.shipping berhasil di transform')
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()
            
            
        logger.debug("Start handling data Null on Manufacture")
        try:
            df_dropped_null['manufacturer'] = df_dropped_null.apply(transform_class.fill_null, axis=1)
            logger.info('Data Manufacture berhasil di transform')
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()

        logger.debug("Start handling data on shipping_cost")
        try:
            df_dropped_null['shipping_cost'] = df_dropped_null['prices.shipping'].apply(transform_class.create_shipping_to_float)
            logger.info('Data shipping_cost berhasil di buat')
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()
        
        logger.debug('Change Data Type date on ')
        try :
            df_dropped_null['dateAdded'] = pd.to_datetime(df_dropped_null['dateAdded'].str.replace('T', ' ').str.replace('Z', ''))
            df_dropped_null['dateUpdated'] = pd.to_datetime(df_dropped_null['dateUpdated'].str.replace('T', ' ').str.replace('Z', ''))
            logger.debug('date berhasil di rubah')
        except ValueError:
            logger.warning('Terjadi Error pada perubahan data to datetime')
        except Exception:
            logger.error()
                
                
        try:
            df_marketing_clean = transform_class.drop_null_data(df_dropped_null)
        except ValueError:
            logger.warning('')
        except Exception:
            logger.error()


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

        try:
            df_marketing_clean = transform_class.rename_col(df_marketing_clean,cols_new)
            logger.info('Data date berhasil di transform')
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()

        # check duplicate and null
        logger.info(transform_class.check_duplicated(df_marketing_clean))
        logger.info(transform_class.check_null(df_marketing_clean))
             
        try:
            df_marketing_clean.to_csv(self.output()['marketing'].path, index=False)
            logger.info("✅ Data Marketing Successfully Extracted")
        except ValueError:
            logger.warning('nama')
        except Exception as e:
            logger.error(f"❌ Error extracting marketing data: {e}")
    
    def output(self):
        return { 'marketing': luigi.LocalTarget("data/transform/extracted_marketing.csv") }

class TransformDataBooks(luigi.Task):
    def requires(self):
        return ExtractBooks()
    
    def run(self):
        input_files = self.input()
        # extracting data books
        
        logger.debug("Transform Data Books")
        logger.debug("Read Data books ke dataframe")

        try:
            data_book_scrap = pd.read_csv(input_files['books'].path)
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()

        
        logger.info(transform_class.check_duplicated(data_book_scrap))
        logger.info(transform_class.check_null(data_book_scrap))

        # Transform price col
        logger.debug('Tranform price to float')
        try:
            data_book_scrap['price'] = data_book_scrap['price'].apply(transform_class.replace_row).astype(float)
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()

        # Trasform rating col
        logger.debug('Transform rationg to int')
        try:
            data_book_scrap['rating'] = data_book_scrap['rating'].apply(transform_class.rating_to_int).astype(int)
        except ValueError:
            logger.warning()
        except Exception:
            logger.error()                

        #extract to csv
        try:
            data_book_scrap.to_csv(self.output()['books'].path, index=False)
            logger.info("✅ Data books Successfully Extracted")
        except ValueError:
            logger.warning('nama')
        except Exception as e:
            logger.error(f"❌ Error extracting marketing data: {e}")
    
    def output(self):
        return {'books': luigi.LocalTarget("data/transform/extracted_books.csv")}



    
    
       