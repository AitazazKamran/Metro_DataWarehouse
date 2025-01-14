# Metro_DataWarehouse
Read-Me: Step-by-Step Guide to Operate the Project
Step 1: Prerequisites
Before operating the project, ensure the following prerequisites are met:
- Install MySQL Server and Workbench.
- Install Java Development Kit (JDK) and configure JAVA_HOME.
- Install any required Java libraries (e.g., MySQL JDBC driver).
- Ensure all SQL script files (e.g., `Create_DB_DataStore.sql`, `Create_DB_MD.sql`, `Create-DW.sql`) are available.
Step 2: Setting Up Databases
1. Open MySQL Workbench and execute the provided SQL scripts in the following order:
- `Create_DB_MD.sql`: Creates the Master Data database and its schema.
- `Create_DB_DataStore.sql`: Creates the Data Store database and its schema.
- `Create-DW.sql`: Creates the Data Warehouse database and its schema.
Step 3: Loading Data
1. Ensure the CSV files (e.g., `products_data.csv`, `customers_data.csv`, `transactions_data.csv`) are correctly placed at the specified paths in the project.
2. Run the following Java programs to load data into the respective databases:
- `master_data_upload.java`: Loads product and customer data into the Master Data database.
- `transactions_uploader.java`: Loads transactional data into the Data Store database.
Step 4: Running the ETL Process
1. Compile and run the `ETL_RUNNER.java` program.
2. The program will extract data from the Data Store and Master Data databases, transform it using the MESHJOIN algorithm, and load it into the Data Warehouse database.
Step 5: Analyzing Data in the Data Warehouse
1. Use the provided `olap_queries.sql` file to perform analysis on the Data Warehouse.
2. Execute the queries in MySQL Workbench or any compatible SQL tool to generate insights.
   - For example, analyze top-selling products, seasonal trends, and supplier contributions.
Step 6: Project Output
1. The Data Warehouse will contain populated dimension and fact tables after the ETL process.
2. OLAP queries will provide insights such as:
- Revenue trends by product, store, and time period.
- Monthly and quarterly performance.
- High sales spikes and product associations.
Step 7: Notes and Troubleshooting
- Ensure the database credentials in the Java programs match your MySQL configuration.
- Check for any missing or malformed data in the CSV files before uploading.
- If the ETL process fails, verify database connections and log messages for errors.
