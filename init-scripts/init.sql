-- Membuat tabel untuk data_book
CREATE TABLE data_books (
    title TEXT,
    price FLOAT,
    link_product TEXT,
    img_url TEXT, 
    rating INT,
    CONSTRAINT check_rating CHECK (rating >= 0 AND rating <= 5)  
);

-- Membuat tabel untuk data_sales
CREATE TABLE data_sales (
    name TEXT,  
    main_category VARCHAR(255),
    sub_category VARCHAR(255),
    image TEXT,
    link TEXT,  
    ratings INT,
    no_of_ratings INT,
    discount_price FLOAT,
    actual_price FLOAT
);

