from tasks.transform_tasks.transform_task import TransformDataSales, TransformDataMarketng, TransformDataBooks
from services.load.load import Load
import luigi
import pandas as pd
import os
from utils.logger import logger_load

load_class = Load()


class LoadDataSales(luigi.Task):
    def requires(self):
        return TransformDataSales()

    def run(self):
        logger = logger_load
        input_files = self.input()

        try:
            df_sales = pd.read_csv(input_files['sales'].path)
            logger.info("📂 File Sales siap diload")

            username = os.getenv("ETL_POSTGRES_USER")
            password = os.getenv("ETL_POSTGRES_PASSWORD")
            host = os.getenv("ETL_HOST")
            port = os.getenv("ETL_PORT")
            database = os.getenv("ETL_POSTGRES_DB")

            conn = load_class.get_conn_from_database(
                username, password, host, port, database
            )

            load_class.load_data_to_db(df_sales, "data_sales", conn)
            logger.info("✅ Selesai load data Sales")
        except Exception as e:
            logger.error(f"❌ Gagal load data Sales: {e}", exc_info=True)

    def output(self):
        return luigi.LocalTarget("logs/load_sales.done")

class LoadDataMarketing(luigi.Task):
    def requires(self):
        return TransformDataMarketng()

    def run(self):
        logger = logger_load
        input_files = self.input()

        try:
            df_marketing = pd.read_csv(input_files['marketing'].path)
            logger.info("📂 File Marketing siap diload")

            username = os.getenv("ETL_POSTGRES_USER")
            password = os.getenv("ETL_POSTGRES_PASSWORD")
            host = os.getenv("ETL_HOST")
            port = os.getenv("ETL_PORT")
            database = os.getenv("ETL_POSTGRES_DB")

            conn = load_class.get_conn_from_database(
                username, password, host, port, database
            )

            load_class.load_data_to_db(df_marketing, "data_marketing", conn)
            logger.info("✅ Selesai load data Marketing")
        except Exception as e:
            logger.error(f"❌ Gagal load data Marketing: {e}", exc_info=True)

    def output(self):
        return luigi.LocalTarget("logs/load_marketing.done")


class LoadDataBooks(luigi.Task):
    def requires(self):
        return TransformDataBooks()

    def run(self):
        logger = logger_load
        input_files = self.input()

        try:
            df_books = pd.read_csv(input_files['books'].path)
            logger.info("📂 File Books siap diload")

            username = os.getenv("ETL_POSTGRES_USER")
            password = os.getenv("ETL_POSTGRES_PASSWORD")
            host = os.getenv("ETL_HOST")
            port = os.getenv("ETL_PORT")
            database = os.getenv("ETL_POSTGRES_DB")

            conn = load_class.get_conn_from_database(
                username, password, host, port, database
            )

            load_class.load_data_to_db(df_books, "data_books", conn)
            logger.info("✅ Selesai load data Books")
        except Exception as e:
            logger.error(f"❌ Gagal load data Books: {e}", exc_info=True)

    def output(self):
        return luigi.LocalTarget("logs/load_books.done")
