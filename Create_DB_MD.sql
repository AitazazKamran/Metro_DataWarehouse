DROP DATABASE IF EXISTS Master_Data;
CREATE DATABASE IF NOT EXISTS Master_Data;
USE Master_Data;

-- Create the 'products' table
CREATE TABLE products (
    Product_ID INT PRIMARY KEY,
	Product_Name VARCHAR(150) NOT NULL,
   Price Decimal(10, 2) NOT NULL,
   Supplier_ID INT,
    Supplier_Name VARCHAR(50) NOT NULL,
    Store_ID INT,
    Store_Name VARCHAR(30) NOT NULL
);


-- Create the 'customers' table
CREATE TABLE customers (
      Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(30) NOT NULL,
    Gender VARCHAR(10) NOT NULL
);
