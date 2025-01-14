use dw_project;
-- Query 1---
SELECT 
    Product.Product_Name,Time_Dimension.Month,Time_Dimension.Weekend,SUM(Sales.Total_Sale) AS Revenue FROM Sales
    JOIN Product ON Product.Product_ID = Sales.Product_ID 
    JOIN Time_Dimension ON Time_Dimension.Time_ID = Sales.Time_ID
    WHERE Time_Dimension.Year = 2019 
GROUP BY Product.Product_ID, Time_Dimension.Month, Time_Dimension.Weekend
ORDER BY Revenue DESC
LIMIT 5;
-- Query 2--
SELECT
    t.Year,t.Quarter,st.Store_Name,
    SUM(s.Total_Sale) AS Total_Revenue FROM Sales s
   JOIN Store st ON s.Store_ID = st.Store_ID
   JOIN Time_Dimension t ON s.Time_ID = t.Time_ID
WHERE t.Year = 2019
GROUP BY t.Year,t.Quarter, st.Store_Name
ORDER BY
    t.Year, t.Quarter, st.Store_Name;

-- query 3--
SELECT
    st.Store_Name, sp.Supplier_Name, p.Product_Name,
    SUM(s.Total_Sale) AS Total_Sales_Contribution
FROM Sales s
JOIN Store st ON s.Store_ID = st.Store_ID
JOIN supplier sp ON s.Supplier_ID = sp.Supplier_ID
JOIN Product p ON s.Product_ID = p.Product_ID
GROUP BY st.Store_Name,sp.Supplier_Name,p.Product_Name
ORDER BY st.Store_Name,sp.Supplier_Name,p.Product_Name;
    
-- Query 4 -- 
SELECT
    CASE
        WHEN t.Month IN (3, 4, 5) THEN 'Spring'
        WHEN t.Month IN (6, 7, 8) THEN 'Summer'
        WHEN t.Month IN (9, 10, 11) THEN 'Fall'
        WHEN t.Month IN (12, 1, 2) THEN 'Winter'
    END AS Season,
    p.Product_Name,
    SUM(s.Total_Sale) AS Total_Seasonal_Sales
FROM
    Sales s
JOIN
    Product p ON s.Product_ID = p.Product_ID
JOIN
    Time_Dimension t ON s.Time_ID = t.Time_ID
GROUP BY
    Season,
    p.Product_Name
ORDER BY
    Season,
    p.Product_Name;   
-- query 5 --
SELECT
    st.Store_Name,
    sp.Supplier_Name,
    t.Year,
    t.Month,
    SUM(s.Total_Sale) AS Total_Monthly_Sales,
    (SUM(s.Total_Sale) - LAG(SUM(s.Total_Sale)) OVER (PARTITION BY st.Store_Name, sp.Supplier_Name ORDER BY t.Year, t.Month)) / NULLIF(LAG(SUM(s.Total_Sale)) OVER (PARTITION BY st.Store_Name, sp.Supplier_Name ORDER BY t.Year, t.Month), 0) * 100 AS Revenue_Volatility_Percentage
FROM
    Sales s
JOIN
    Store st ON s.Store_ID = st.Store_ID
JOIN
    Supplier sp ON s.Supplier_ID = sp.Supplier_ID
JOIN
    Time_Dimension t ON s.Time_ID = t.Time_ID
GROUP BY
    st.Store_Name,
    sp.Supplier_Name,
    t.Year,
    t.Month
ORDER BY
    st.Store_Name,
    sp.Supplier_Name,
    t.Year,
    t.Month;

-- Query 6--
SELECT
    p1.Product_Name AS Product_A,
    p2.Product_Name AS Product_B,
    COUNT(*) AS Pair_Count
FROM
    Sales s1
JOIN
    Sales s2 ON s1.Order_ID = s2.Order_ID AND s1.Product_ID < s2.Product_ID
JOIN
    Product p1 ON s1.Product_ID = p1.Product_ID
JOIN
    Product p2 ON s2.Product_ID = p2.Product_ID
GROUP BY
    p1.Product_Name,
    p2.Product_Name
ORDER BY
    Pair_Count DESC
LIMIT 5;
-- Query 7 --
SELECT
    t.Year,
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name,
    SUM(s.Total_Sale) AS Total_Revenue
FROM
    Sales s
JOIN
    Store st ON s.Store_ID = st.Store_ID
JOIN
    Supplier sp ON s.Supplier_ID = sp.Supplier_ID
JOIN
    Product p ON s.Product_ID = p.Product_ID
JOIN
    Time_Dimension t ON s.Time_ID = t.Time_ID
GROUP BY
    ROLLUP(t.Year, st.Store_Name, sp.Supplier_Name, p.Product_Name)
ORDER BY
    t.Year,
    st.Store_Name,
    sp.Supplier_Name,
    p.Product_Name;

-- query 8--
SELECT
    t.Year,
    p.Product_Name,CASE WHEN t.Half_Of_Year = 1 THEN 'H1' ELSE 'H2' END AS Half_Year,
    SUM(s.Total_Sale) AS Total_Revenue,SUM(s.Quantity) AS Total_Quantity_Sold
FROM
    Sales s
JOIN
    Product p ON s.Product_ID = p.Product_ID
JOIN
    Time_Dimension t ON s.Time_ID = t.Time_ID
GROUP BY
    t.Year,
    p.Product_Name,
    t.Half_Of_Year
ORDER BY
    t.Year,
    p.Product_Name,
    Half_Year;

-- Query 9 --
WITH DailyAverage AS (
    SELECT
        t.Transaction_Date,
        p.Product_Name,
        AVG(s.Total_Sale) OVER (PARTITION BY p.Product_ID) AS Daily_Average_Sales,
        s.Total_Sale
    FROM
        Sales s
    JOIN
        Product p ON s.Product_ID = p.Product_ID
    JOIN
        Time_Dimension t ON s.Time_ID = t.Time_ID
),
HighSpikes AS (
    SELECT
        Transaction_Date,
        Product_Name,
        Total_Sale,
        Daily_Average_Sales
    FROM
        DailyAverage
    WHERE
        Total_Sale > 2 * Daily_Average_Sales
)
SELECT
    Transaction_Date,
    Product_Name,
    Total_Sale,
    Daily_Average_Sales,
    (Total_Sale - Daily_Average_Sales) AS Sales_Deviation
FROM
    HighSpikes
ORDER BY
    Product_Name,
    Transaction_Date;

-- Explanation:
-- 1. The DailyAverage CTE calculates the daily average sales for each product.
-- 2. We use a window function (AVG() OVER PARTITION BY) to get the average sales by product.
-- 3. The HighSpikes CTE identifies days where sales exceed twice the daily average sales for each product.
-- 4. The main query selects details of these high sales spikes, including the deviation from the average.
-- 5. The results are ordered by Product and Transaction_Date to help analyze any unusual demand events.

-- Query 10 --
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    st.Store_Name,
    t.Year,
    t.Quarter,
    SUM(s.Total_Sale) AS Total_Quarterly_Sales
FROM
    Sales s
JOIN
    Store st ON s.Store_ID = st.Store_ID
JOIN
    Time_Dimension t ON s.Time_ID = t.Time_ID
GROUP BY
    st.Store_Name,
    t.Year,
    t.Quarter
ORDER BY
    st.Store_Name,
    t.Year,
    t.Quarter;
