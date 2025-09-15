from tasks.transform_tasks.transform_task import TransformDataSales, TransformDataMarketng, TransformDataBooks
from services.load.load import Load
import luigi
import pandas as pd
import os
from utils.logger import logger_load

load_class = Load()


def get_path(target):
    if isinstance(target, dict):
        return get_path(next(iter(target.values())))
    elif isinstance(target, list):
        return get_path(target[0])
    else:
        return target.path


class LoadAllData(luigi.Task):
    def requires(self):
        return {
            "sales": TransformDataSales(),
            "marketing": TransformDataMarketng(),
            "books": TransformDataBooks()
        }

    def run(self):
        logger = logger_load
        logger.info("=== Mulai proses Load semua data ===")

        try:
            input_files = self.input()

            df_sales = pd.read_csv(get_path(input_files['sales']))
            df_marketing = pd.read_csv(get_path(input_files['marketing']))
            df_books = pd.read_csv(get_path(input_files['books']))
            logger.info("Berhasil membaca semua file hasil transform")

            # Database connection
            username = os.getenv('ETL_POSTGRES_USER')
            password = os.getenv('ETL_POSTGRES_PASSWORD')
            host = os.getenv('ETL_HOST')
            port = os.getenv('ETL_PORT')
            database = os.getenv('ETL_POSTGRES_DB')

            conn = load_class.get_conn_from_database(username, password, host, port, database)
            logger.info("Koneksi database berhasil dibuat")
        except Exception as e:
            logger.error(f"Gagal menyiapkan data atau koneksi DB: {e}", exc_info=True)
            raise  # biar ga lanjut ke bawah

        # Load data Sales
        try:
            logger.info("Mulai load data sales...")
            load_class.load_data_to_db(df_sales, "data_sales", conn)
            logger.info("✅ Selesai load data sales")
        except Exception as e:
            logger.error(f"❌ Gagal load data sales: {e}", exc_info=True)

        # Load data Marketing
        try:
            logger.info("Mulai load data marketing...")
            load_class.load_data_to_db(df_marketing, "data_marketing", conn)
            logger.info("✅ Selesai load data marketing")
        except Exception as e:
            logger.error(f"❌ Gagal load data marketing: {e}", exc_info=True)

        # Load data Books
        try:
            logger.info("Mulai load data books...")
            load_class.load_data_to_db(df_books, "data_book", conn)
            logger.info("✅ Selesai load data books")
        except Exception as e:
            logger.error(f"❌ Gagal load data books: {e}", exc_info=True)

        # marker file biar Luigi tau task selesai
        with self.output().open("w") as f:
            f.write("Load selesai sebagian/semua")

        logger.info("=== Proses Load selesai ===")

    def output(self):
        return luigi.LocalTarget("logs/load_all.done")
