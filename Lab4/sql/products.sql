CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(10) UNIQUE,
    product_description VARCHAR(255)
);