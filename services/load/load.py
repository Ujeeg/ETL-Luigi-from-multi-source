import pandas as pd
from sqlalchemy import create_engine
class Load:
    def __init__(self):
        pass
    
    def get_conn_from_database(self, username, password, host, port, database):
        url =f'postgresql://{username}:{password}@{host}:{port}/{database}'
        engine = create_engine(url)
        return engine

    def load_data_to_db(self, df, table_name, connection):
        try:
            hasil = df.to_sql(table_name, con = connection, if_exists="append", index=False)
            print(f"load data {table_name} berhasil sebanyak {len(df)}")

        except:
            print("error {terjadi kesalahan pada bentuk data}")