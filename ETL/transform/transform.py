import pandas as pd


class Transform:
    
    
    def __init__(self):
        None
        
    def check_null(self, df):
        df_null = df.isnull().sum().reset_index()
        df_null.columns = ['col', 'missing_val']
        df_null_col = df_null[df_null['missing_val'] > 0].copy()
        df_null_col['missing_val'] = df_null_col['missing_val'].astype(int)
        df_null_col['missing_value_percentage'] = round((df_null_col['missing_val'] / len(df)) * 100,2)
        return df_null_col
    
    def check_duplicated(self, df):
        df_cheked = df.duplicated().sum()
        return f'total data duplicate {df_cheked}'
    
    def drop_columns_null(self, df,col):
        df_clear = df.drop(columns=col)
        return df_clear
    
    def get_colums_null_list(self, df):
        df = df[df['missing_value_percentage'] > 60]
        col = list(df.col)
        return col
    
    def fill_shipping(self, row, row_1, row_2):
        if pd.isna(row[row_1]) and row[row_2] > 45:
            return 'Free Expedited Shipping for most orders over $49'
        elif pd.isna(row[row_1]) and row[row_2] > 35:
            return 'Free Expedited Shipping for most orders over $35'
        else:
            return row[row_1]
    
    def fill_null(self, row):
        if pd.isna(row['manufacturer']):
            return row['brand']
        else:
            return row['manufacturer']
        
    def create_shipping_to_float(self, col):
        if not isinstance(col, str):
            return 0.0
        
        if 'USD' in col:
            parts = col.replace('USD', '').strip().split()
            for part in parts:
                try:
                    return float(part)
                except ValueError:
                    continue
        return 0.0

    def drop_null_data(self, df):
        # Drop baris yang memiliki nilai NaN di semua kolom
        df_clean = df.dropna().reset_index(drop=True)
        return df_clean

    def drop_duplicate(self, df):
        # Drop baris yang duplikat
        df_clean = df.drop_duplicates().reset_index(drop=True)
        return df_clean
    def rename_col(self,df,col_format):
        df = df.rename(columns=col_format)
        return df
    
    def replace_row(self,price):
        string = str(price)
        float_data = string.replace('Â£','')
        return float_data
    
    def rating_to_int(self,col):
        if col == 'One':
            return 1
        elif col == 'Two':
            return 2
        elif col == 'Three':
            return 3
        elif col == 'Four':
            return 4
        elif col == 'Five':
            return 5