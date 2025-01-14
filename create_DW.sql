DROP DATABASE IF EXISTS DW_Project;
CREATE DATABASE DW_Project;
USE DW_Project;

CREATE TABLE Supplier (
    Supplier_ID INT PRIMARY KEY,
    Supplier_Name VARCHAR(50) NOT NULL
);

CREATE TABLE Store (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(30) NOT NULL
);

CREATE TABLE Customer (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(30) NOT NULL,
    Gender VARCHAR(10) NOT NULL
);

CREATE TABLE Product (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(150) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE Time_Dimension (
    Time_ID INT PRIMARY KEY,
    Transaction_Date DATE NOT NULL,
    Weekend TINYINT(1) NOT NULL, 
    Time TIME NOT NULL,
    Half_Of_Year INT NOT NULL CHECK (Half_Of_Year IN (1, 2)),
    Month INT NOT NULL CHECK (Month BETWEEN 1 AND 12),
    Quarter INT NOT NULL CHECK (Quarter BETWEEN 1 AND 4),
    Year INT NOT NULL
);

CREATE TABLE Sales (
    Order_ID INT NOT NULL,
    Product_ID INT NOT NULL,
    Customer_ID INT NOT NULL,
    Supplier_ID INT NOT NULL,
    Time_ID INT NOT NULL,
    Store_ID INT NOT NULL,
    Quantity INT NOT NULL,
    Total_Sale DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (Order_ID, Product_ID, Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES Product (Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customer (Customer_ID),
    FOREIGN KEY (Supplier_ID) REFERENCES Supplier (Supplier_ID),
    FOREIGN KEY (Time_ID) REFERENCES Time_Dimension (Time_ID),
    FOREIGN KEY (Store_ID) REFERENCES Store (Store_ID)
);

