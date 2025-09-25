import pandas as pd
import numpy as np


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
        df = df.dropna()
        return df


    def drop_duplicate(self, df):
        # Drop baris yang duplikat
        df.drop_duplicates(inplace=True)
    
    def rename_col(self,df,col_format):
        df = df.rename(columns=col_format)
        return df
    
    def replace_row(self,price):
        string = str(price)
        float_data = string.replace('Â£','')
        return float_data
    
    def replace_row_sales(self, price):
        string = str(price)
        try:
            float_data = string.replace('₹', '').replace(',', '').replace(' ', '')
            float_data = float(float_data)
            return float_data
        except ValueError:  
            return 0.0  

    
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
        
    def change_availability(self,data):
        instock = ["in stock",'yes', 'true']
        outstock  = ["out of stock","no","sold","retired"]
        preorder = ["special order","more on the way"]

        data = str(data).lower()
        if data in instock:
            return 'In Stock'
        elif data in outstock:
            return 'Out of Stock'
        elif data in preorder:
            return 'PreOrder'
        else:
            return 'Unknown'

    def change_condition(self,data):
        new = ["new", "brand new", "condition: brand new!", "brand new!", "new other (see details)"]
        used  = [ "used","pre-owned","preowned"]
        refurbished = [ "manufacturer refurbished", "seller refurbished", "refurbished"]

        data = str(data).lower()
        if data in new:
            return 'New'
        elif data in used:
            return 'Used'
        elif data in refurbished:
            return 'Refurbished'
        else:
            return 'Unknown'