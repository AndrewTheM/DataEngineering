CREATE TABLE transactions (
    transaction_id VARCHAR(32) PRIMARY KEY,
    transaction_date DATE,
    product_id INT REFERENCES products(product_id),
    product_code VARCHAR(10),
    product_description VARCHAR(255),
    quantity INT,
    account_id INT REFERENCES accounts(customer_id)
);