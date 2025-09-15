from tasks.extract_tasks.extract_task import ExtractBooks, ExtractMarketing, ExtractSales
from services.transform.transform import Transform
import luigi
import pandas as pd
import os
from utils.logger import logger_transform

transform_class = Transform()

logger = logger_transform



class TransformDataSales(luigi.Task):

    def requires(self):
        return ExtractSales()

    def run(self):
        input_files = self.input()

        logger.debug("📂 Membaca data Sales ke DataFrame")
        try:
            df_sales = pd.read_csv(input_files['sales'].path)
            logger.info("✅ File Sales CSV berhasil dibaca")
        except ValueError:
            logger.warning('❌ ValueError saat membaca file Sales CSV')
        except Exception as e:
            logger.error(f'❌ Gagal membaca file Sales CSV: {e}')

        logger.debug('🔍 Mengecek nilai null pada df_sales')
        logger.info(transform_class.check_null(df_sales))

        logger.debug('🗑️ Menghapus data null < 1% dari df_sales')
        try:
            df_sales_drop = transform_class.drop_null_data(df_sales)
        except Exception as e:
            logger.error(f'❌ Error drop_null_data Sales: {e}')

        logger.debug("🗑️ Menghapus kolom 'Unnamed: 0'")
        try:
            df_sales_drop = transform_class.drop_columns_null(df_sales_drop, 'Unnamed: 0')
        except Exception as e:
            logger.error(f'❌ Error drop_columns_null Sales: {e}')

        logger.debug('🔄 Transform kolom discount_price, actual_price, no_of_ratings, ratings')
        try:
            df_sales_drop['discount_price'] = df_sales_drop['discount_price'].apply(transform_class.replace_row_sales)
            df_sales_drop['actual_price'] = df_sales_drop['actual_price'].apply(transform_class.replace_row_sales)
            df_sales_drop['no_of_ratings'] = df_sales_drop['no_of_ratings'].apply(transform_class.replace_row_sales).astype(int)
            df_sales_drop['ratings'] = df_sales_drop['ratings'].apply(transform_class.replace_row_sales).astype(int)
            df_sales_clean = df_sales_drop
        except Exception as e:
            logger.error(f'❌ Error transform kolom Sales: {e}')

        logger.debug("🔍 Cek kembali data null setelah transform")
        logger.info(transform_class.check_null(df_sales_clean))

        try:
            df_sales_clean.to_csv(self.output()['sales'].path, index=False)
            logger.info("✅ Data Sales berhasil ditransformasi & disimpan")
        except Exception as e:
            logger.error(f"❌ Error simpan Sales: {e}")

    def output(self):
        return {'sales': luigi.LocalTarget("data/transform/transform_sales.csv")}


class TransformDataMarketng(luigi.Task):

    def requires(self):
        return ExtractMarketing()

    def run(self):
        input_files = self.input()

        logger.debug("📂 Membaca data Marketing ke DataFrame")
        try:
            df_marketing = pd.read_csv(input_files['marketing'].path)
            logger.info('✅ File Marketing CSV berhasil dibaca')
        except ValueError:
            logger.warning('❌ ValueError saat membaca file Marketing CSV')
        except Exception as e:
            logger.error(f'❌ Gagal membaca file Marketing CSV: {e}')

        logger.debug('🔍 Validasi data Marketing (null & duplicate)')
        try:
            logger.info(transform_class.check_null(df_marketing))
            df_marketing_null = transform_class.check_null(df_marketing)
        except Exception as e:
            logger.error(f'❌ Error check_null Marketing: {e}')

        try:
            logger.info(transform_class.check_duplicated(df_marketing))
        except Exception as e:
            logger.error(f'❌ Error check_duplicated Marketing: {e}')

        logger.debug('📊 Mendapatkan kolom dengan null > 60%')
        try:
            null_columns = transform_class.get_colums_null_list(df_marketing_null)
            df_dropped_null = transform_class.drop_columns_null(df_marketing, null_columns)
            logger.info(f'🗑️ Kolom {null_columns} berhasil dihapus')
        except Exception as e:
            logger.error(f'❌ Error drop_columns_null Marketing: {e}')

        logger.debug("🛠️ Transform kolom prices.shipping")
        try:
            df_dropped_null['prices.shipping'] = df_dropped_null.apply(
                transform_class.fill_shipping,
                axis=1,
                args=('prices.shipping', 'prices.amountMin')
            )
            logger.info('✅ prices.shipping berhasil ditransformasi')
        except Exception as e:
            logger.error(f'❌ Error transformasi prices.shipping: {e}')

        logger.debug("🛠️ Isi kolom manufacturer")
        try:
            df_dropped_null['manufacturer'] = df_dropped_null.apply(transform_class.fill_null, axis=1)
            logger.info('✅ manufacturer berhasil ditransformasi')
        except Exception as e:
            logger.error(f'❌ Error transformasi manufacturer: {e}')

        logger.debug("🛠️ Buat kolom shipping_cost (float)")
        try:
            df_dropped_null['shipping_cost'] = df_dropped_null['prices.shipping'].apply(transform_class.create_shipping_to_float)
            logger.info('✅ shipping_cost berhasil dibuat')
        except Exception as e:
            logger.error(f'❌ Error buat shipping_cost: {e}')

        logger.debug("🗓️ Konversi kolom dateAdded & dateUpdated ke datetime")
        try:
            df_dropped_null['dateAdded'] = pd.to_datetime(df_dropped_null['dateAdded'].str.replace('T', ' ').str.replace('Z', ''))
            df_dropped_null['dateUpdated'] = pd.to_datetime(df_dropped_null['dateUpdated'].str.replace('T', ' ').str.replace('Z', ''))
            logger.info('✅ dateAdded & dateUpdated berhasil dikonversi')
        except Exception as e:
            logger.error(f'❌ Error konversi date: {e}')

        logger.debug("🧹 Menghapus sisa data null")
        try:
            df_marketing_clean = transform_class.drop_null_data(df_dropped_null)
        except Exception as e:
            logger.error(f'❌ Error drop_null_data Marketing: {e}')

        logger.debug("🏷️ Rename kolom Marketing")
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
            df_marketing_clean = transform_class.rename_col(df_marketing_clean, cols_new)
            logger.info('✅ Kolom Marketing berhasil di-rename')
        except Exception as e:
            logger.error(f'❌ Error rename kolom Marketing: {e}')

        logger.info(transform_class.check_duplicated(df_marketing_clean))
        logger.info(transform_class.check_null(df_marketing_clean))

        try:
            df_marketing_clean.to_csv(self.output()['marketing'].path, index=False)
            logger.info("✅ Data Marketing berhasil ditransformasi & disimpan")
        except Exception as e:
            logger.error(f"❌ Error simpan Marketing: {e}")

    def output(self):
        return {'marketing': luigi.LocalTarget("data/transform/transform_marketing.csv")}


class TransformDataBooks(luigi.Task):
    def requires(self):
        return ExtractBooks()

    def run(self):
        input_files = self.input()

        logger.debug("📂 Membaca data Books ke DataFrame")
        try:
            data_book_scrap = pd.read_csv(input_files['books'].path)
            logger.info("✅ File Books CSV berhasil dibaca")
        except ValueError:
            logger.warning("❌ ValueError saat membaca file Books CSV")
        except Exception as e:
            logger.error(f'❌ Gagal membaca file Books CSV: {e}')

        logger.debug('🔍 Mengecek data duplicate & null')
        logger.info(transform_class.check_duplicated(data_book_scrap))
        logger.info(transform_class.check_null(data_book_scrap))

        logger.debug('🔄 Transform kolom price ke float')
        try:
            data_book_scrap['price'] = data_book_scrap['price'].apply(transform_class.replace_row).astype(float)
            logger.info("✅ Kolom price berhasil ditransformasi")
        except Exception as e:
            logger.error(f'❌ Error transform price: {e}')

        logger.debug('🔄 Transform kolom rating ke int')
        try:
            data_book_scrap['rating'] = data_book_scrap['rating'].apply(transform_class.rating_to_int).astype(int)
            logger.info("✅ Kolom rating berhasil ditransformasi")
        except Exception as e:
            logger.error(f'❌ Error transform rating: {e}')

        try:
            data_book_scrap.to_csv(self.output()['books'].path, index=False)
            logger.info("✅ Data Books berhasil ditransformasi & disimpan")
        except Exception as e:
            logger.error(f"❌ Error simpan Books: {e}")

    def output(self):
        return {'books': luigi.LocalTarget("data/transform/transform_books.csv")}
