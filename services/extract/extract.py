import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine


class Extract:

# scrapping function from web
  
    def scrap_book_data(self, jumlah_web, base_url_template, base_site, base_img):
        
        """
        Scrape book data from the 'Books to Scrape' website across multiple pages.

        This function fetches book details such as title, price, product link, image link,
        and rating from the specified number of pages and returns the data in a pandas DataFrame.

        Parameters:
            jumlah_web (int): Number of pages to scrape (starting from page 1).
            base_url_template (str): URL template containing a placeholder '{}' for page number.
                                    Example: 'https://books.toscrape.com/catalogue/page-{}.html'
            base_site (str): Base URL for constructing the full product link.
                            Example: 'https://books.toscrape.com/catalogue/'
            base_img (str): Base URL for constructing the full image link.
                            Example: 'https://books.toscrape.com/'

        Returns:
            pandas.DataFrame: A DataFrame containing the scraped data with the following columns:
                - title (str): Book title
                - price (str): Book price
                - link_product (str): Full URL of the product page
                - img_url (str): Full URL of the product image
                - rating (str): Book rating (e.g., 'Three', 'Four')

        Example:
            extractor = Extract()
            BASE_TEMPLATE = "https://books.toscrape.com/catalogue/page-{}.html"
            BASE_SITE = "https://books.toscrape.com/catalogue/"
            BASE_IMG = "https://books.toscrape.com/"

            df_books = extractor.scrap_book_data(5, BASE_TEMPLATE, BASE_SITE, BASE_IMG)
            print(df_books.head())
        """

        
        all_data = []
        
        for i in range(1, jumlah_web + 1):
            url = base_url_template.format(i)
            resp = requests.get(url)
            
            if resp.status_code != 200:
                print(f"Gagal akses halaman {i}")
                continue
            
            print(f"Status code {resp.status_code}, halaman {i} aman")
            
            soup = BeautifulSoup(resp.text, "html.parser")
            books = soup.select(".product_pod")
            
            for book in books:
                judul = book.h3.a['title']
                price = book.select_one(".price_color").text
                relative_link = book.h3.a['href']
                link_produk = base_site + relative_link
                relative_img = book.img['src'].replace("../../", "")
                img_url = base_img + relative_img
                rating = book.select_one(".star-rating")['class'][1]
                
                all_data.append({
                    "title": judul,
                    "price": price,
                    "link_product": link_produk,
                    "img_url": img_url,
                    "rating": rating
                })
        
        return pd.DataFrame(all_data)



# extract data sales from database
    def connection_to_database(self, url, table):
        """
        Connect to a database using the given URL and retrieve all data from the specified table.

        Parameters:
            url (str): The database connection string in the format
                    'postgresql://username:password@host:port/database'.
            table (str): The name of the table to query.

        Returns:
            pandas.DataFrame: A DataFrame containing all rows from the specified table,
                            if the connection and query are successful.
            str: "error_connection" if the connection or query fails.

        Example:
            db_url = "postgresql://postgres:12345@localhost:5432/mydb"
            table_name = "books"
            df = connection_to_database(db_url, table_name)
        """
        query = f"SELECT * FROM {table}"
        conn = create_engine(url)
        df = pd.read_sql(query, conn)
        return df


    def extract_data_csv(self, file_path):
        """
    Load data from a CSV file into a pandas DataFrame.

    This function attempts to read a CSV file from the specified file path.
    If the file cannot be read due to a ValueError, it returns the string "data Error".

    Parameters:
        file_path (str): The full path or relative path to the CSV file.

    Returns:
        pandas.DataFrame: A DataFrame containing the CSV data if successful.
        str: "data Error" if reading the CSV file fails due to a ValueError.

    Example:
        extractor = Extract()
        df = extractor.extract_data_csv("data/books.csv")
        if isinstance(df, str):
            print("Error:", df)
        else:
            print(df.head())
    """
        df = pd.read_csv(file_path)
        return df