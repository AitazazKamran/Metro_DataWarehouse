DROP DATABASE IF EXISTS Data_Store;
CREATE DATABASE IF NOT EXISTS Data_Store;
USE Data_Store;


CREATE TABLE TRANSACTIONS(
  Order_ID INT  , 
  Transaction_Date varchar(50) NOT NULL,
  Product_ID INT NOT NULL,
 Quantity INT NOT NULL,
Customer_ID INT NOT NULL
);

SELECT * 
FROM TRANSACTIONS 
ORDER BY Order_ID DESC 
LIMIT 50;

