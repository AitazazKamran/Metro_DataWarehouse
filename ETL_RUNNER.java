package ETL;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Objects;
//import java.util.Scanner;

public class ETL_RUNNER {
    static String db_name = "Master_Data";
    static String db_usrnme = "root";
    static String pass = "1234";
    static String db2_name = "data_store";
    static String db2_usrnme = "root";
    static String password = "1234";
    static String dw_name = "DW_Project";
    static String dw_usrnme = "root";
    static String passwordd = "1234";

    // Stream class to read transactions from the staging database
    public static class stream extends Thread {

        public class Datasource {
            Integer order_id;
            String order_date;
            Integer product_id;
            Integer customer_id;
            Integer quantity;

            public Datasource(int order_id, String order_date, Integer product_id, Integer customer_id, Integer quantity) {
                this.order_id = order_id;
                this.order_date = order_date;
                this.product_id = product_id;
                this.customer_id = customer_id;
                this.quantity = quantity;
            }
        }

        static Queue<Datasource> stream_buffer = new ConcurrentLinkedQueue<>();

        @Override
        public void run() {
            try {
                String db_url = "jdbc:mysql://127.0.0.1:3306/" + db2_name;
                Connection c = DriverManager.getConnection(db_url, db_usrnme, pass);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM data_store.TRANSACTIONS");

                while (!interrupted() && rs.next()) {
                    Integer order_id = rs.getInt("Order_ID");
                    String order_date = rs.getString("Transaction_Date");
                    Integer product_id = rs.getInt("Product_ID");
                    Integer customer_id = rs.getInt("Customer_ID");
                    Integer quantity = rs.getInt("Quantity");

                    Datasource ds = new Datasource(order_id, order_date, product_id, customer_id, quantity);
                    stream_buffer.add(ds); // Add to queue
                }
                System.out.println("Total transactions read from stream: " + stream_buffer.size());
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    // MasterData class to fetch products and customers from the staging database
    public static class masterData extends Thread {

        public class Product {
            Integer productId;
            String productName;
            Double productPrice;
            Integer supplierId;
            String supplierName;
            Integer storeId;
            String storeName;

            public Product(Integer productId, String productName, Double productPrice, Integer supplierId,
                    String supplierName, Integer storeId, String storeName) {
                this.productId = productId;
                this.productName = productName;
                this.productPrice = productPrice;
                this.supplierId = supplierId;
                this.supplierName = supplierName;
                this.storeId = storeId;
                this.storeName = storeName;
            }
        }

        public class Customer {
            Integer customerId;
            String customerName;
            String customerGender;

            public Customer(int customerId, String customerName, String customerGender) {
                this.customerId = customerId;
                this.customerName = customerName;
                this.customerGender = customerGender;
            }
        }

        static Queue<Customer> md_buffer_cus = new ConcurrentLinkedQueue<>();
        static Queue<Product> md_buffer_pdt = new ConcurrentLinkedQueue<>();

        @Override
        public void run() {
            try {
                String db_url = "jdbc:mysql://127.0.0.1:3306/" + db_name;
                Connection c = DriverManager.getConnection(db_url, db_usrnme, pass);

                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM Master_Data.customers");

                while (!interrupted() && rs.next()) {
                    Integer customer_id = rs.getInt("Customer_ID");
                    String customer_name = rs.getString("Customer_Name");
                    String customerGender = rs.getString("Gender");

                    Customer cus = new Customer(customer_id, customer_name, customerGender);
                    md_buffer_cus.add(cus); // Add to queue
                }
                System.out.println("Total customers loaded: " + md_buffer_cus.size());

                ResultSet rs2 = s.executeQuery("SELECT * FROM Master_Data.products");

                while (!interrupted() && rs2.next()) {
                    Integer product_id = rs2.getInt("Product_ID");
                    String product_name = rs2.getString("Product_Name");
                    Integer supplier_id = rs2.getInt("Supplier_ID");
                    String supplier_name = rs2.getString("Supplier_Name");
                    Double product_price = rs2.getDouble("Price");
                    Integer storeId = rs2.getInt("Store_ID");
                    String storeName = rs2.getString("Store_Name");

                    Product product = new Product(product_id, product_name, product_price, supplier_id, supplier_name,
                            storeId, storeName);
                    md_buffer_pdt.add(product); // Add to queue
                }
                System.out.println("Total products loaded: " + md_buffer_pdt.size());

            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    static HashMap<CompositeKey, transdata> Hashtable = new HashMap<>();

    // Composite key class to store unique transactions in the Hashtable
    public static class CompositeKey {
        Integer order_id;
        Integer product_id;
        Integer customer_id;

        public CompositeKey(Integer order_id, Integer product_id, Integer customer_id) {
            this.order_id = order_id;
            this.product_id = product_id;
            this.customer_id = customer_id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeKey that = (CompositeKey) o;
            return Objects.equals(order_id, that.order_id) &&
                    Objects.equals(product_id, that.product_id) &&
                    Objects.equals(customer_id, that.customer_id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(order_id, product_id, customer_id);
        }
    }

    // Transformer class to implement MESHJOIN
    public static class Transformer extends Thread {
        @Override
        public void run() {
            System.out.println("Transforming and Matching Data using MESHJOIN...");

            int totalTransformed = 0;
            while (!ETL_RUNNER.stream.stream_buffer.isEmpty()) {
                ETL_RUNNER.stream.Datasource ds = ETL_RUNNER.stream.stream_buffer.poll();
                if (ds == null) {
                    continue;
                }

                boolean matchedProduct = false;
                boolean matchedCustomer = false;

                for (ETL_RUNNER.masterData.Product productPartition : ETL_RUNNER.masterData.md_buffer_pdt) {
                    if (productPartition.productId.equals(ds.product_id)) {
                        matchedProduct = true;
                        for (ETL_RUNNER.masterData.Customer customerPartition : ETL_RUNNER.masterData.md_buffer_cus) {
                            if (customerPartition.customerId.equals(ds.customer_id)) {
                                matchedCustomer = true;
                                CompositeKey key = new CompositeKey(ds.order_id, ds.product_id, ds.customer_id);
                                ETL_RUNNER.transdata transaction = new ETL_RUNNER.transdata(
                                        ds,
                                        productPartition.productName,
                                        productPartition.storeName,
                                        productPartition.storeId,
                                        productPartition.supplierId,
                                        productPartition.supplierName,
                                        productPartition.productPrice,
                                        customerPartition.customerName
                                );

                                // Log if a transaction is added to the Hashtable
                                if (ETL_RUNNER.Hashtable.containsKey(key)) {
                               //     System.out.println("Duplicate transaction detected for Order_ID: " + transaction.transaction_id + ", Product_ID: " + transaction.product_id + ", Customer_ID: " + transaction.customer_id);
                                } else {
                                    ETL_RUNNER.Hashtable.put(key, transaction);
                                 //   System.out.println("Transaction added: Order_ID=" + transaction.transaction_id + ", Product_ID=" + transaction.product_id + ", Customer_ID=" + transaction.customer_id);
                                    totalTransformed++;
                                }
                            }
                        }
                    }
                }

                if (!matchedProduct) {
                   // System.out.println("No matching product found for Product ID: " + ds.product_id + " in Order ID: " + ds.order_id);
                }
                if (!matchedCustomer) {
                  //  System.out.println("No matching customer found for Customer ID: " + ds.customer_id + " in Order ID: " + ds.order_id);
                }
            }
            System.out.println("Total transactions transformed: " + totalTransformed);
        }
    }


    public static class transdata {
        Integer transaction_id; // Maps to Order_ID
        Integer product_id;
        Integer customer_id;
        Integer time_id; // Maps to Time_ID
        Integer store_id;
        String store_name;
        String order_date;
        Integer quantity;
        String product_name;
        Integer supplier_id;
        String supplier_name;
        Double price;
        Double sale; // Maps to Total_Sale
        String customer_name;

        public transdata(ETL_RUNNER.stream.Datasource ds_obj, String prod_name, String store_name, Integer store_id,
                Integer sup_id, String sup_name, Double prc, String cus_name) {
            this.transaction_id = ds_obj.order_id; // Maps to Order_ID
            this.product_id = ds_obj.product_id;
            this.customer_id = ds_obj.customer_id;
            this.time_id = ds_obj.order_id; // Example logic for Time_ID
            this.store_id = store_id;
            this.store_name = store_name;
            this.order_date = ds_obj.order_date;
            this.quantity = ds_obj.quantity;
            this.product_name = prod_name;
            this.supplier_id = sup_id;
            this.supplier_name = sup_name;
            this.price = prc;
            this.sale = this.quantity * this.price; // Calculate Total_Sale
            this.customer_name = cus_name;
        }
    }

    public static void main(String[] args) throws ParseException {
    //	Scanner scanner = new Scanner(System.in);
    	// Prompt for database credentials
        System.out.print("Enter Data Warehouse DB name: ");
      //  dw_name = scanner.nextLine();
        System.out.print("Enter Data Warehouse DB username: ");
      //  dw_usrnme = scanner.nextLine();
        System.out.print("Enter Data Warehouse DB password: ");
      //  passwordd = scanner.nextLine();

        System.out.print("Enter Master Data DB name: ");
      //  db_name = scanner.nextLine();
        System.out.print("Enter Master Data DB username: ");
      //  db_usrnme = scanner.nextLine();
        System.out.print("Enter Master Data DB password: ");
      //  pass = scanner.nextLine();

        System.out.print("Enter Transaction DB name: ");
       // db2_name = scanner.nextLine();
        System.out.print("Enter Transaction DB username: ");
      //  db2_usrnme = scanner.nextLine();
        System.out.print("Enter Transaction DB password: ");
       // password = scanner.nextLine();
        stream extractor = new stream();
        masterData mdExtractor = new masterData();
        Transformer transformer = new ETL_RUNNER.Transformer();

        extractor.start();
        mdExtractor.start();

        try {
            extractor.join();
            mdExtractor.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transformer.start();

        try {
            transformer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Preview Completed. Proceeding to Load...");
        loadToWarehouse(); // Load transformed data to warehouse
        System.out.println("ETL Process Completed!");
    }

    // Method to populate Dimension and Fact tables
    public static void loadToWarehouse() throws ParseException {
        String db_url = "jdbc:mysql://127.0.0.1:3306/" + dw_name;
        try (Connection c = DriverManager.getConnection(db_url, dw_usrnme, passwordd)) {
            // Load Product Dimension
            PreparedStatement pstmtProduct = c.prepareStatement(
                    "INSERT INTO Product (Product_ID, Product_Name, Price) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE Product_ID=Product_ID");
            for (masterData.Product product : masterData.md_buffer_pdt) {
                pstmtProduct.setInt(1, product.productId);
                pstmtProduct.setString(2, product.productName);
                pstmtProduct.setDouble(3, product.productPrice);
                pstmtProduct.addBatch();
            }
            pstmtProduct.executeBatch();

            // Load Customer Dimension
            PreparedStatement pstmtCustomer = c.prepareStatement(
                    "INSERT INTO Customer (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE Customer_ID=Customer_ID");
            for (masterData.Customer customer : masterData.md_buffer_cus) {
                pstmtCustomer.setInt(1, customer.customerId);
                pstmtCustomer.setString(2, customer.customerName);
                pstmtCustomer.setString(3, customer.customerGender);
                pstmtCustomer.addBatch();
            }
            pstmtCustomer.executeBatch();

            // Load Supplier Dimension
            PreparedStatement pstmtSupplier = c.prepareStatement(
                    "INSERT INTO Supplier (Supplier_ID, Supplier_Name) VALUES (?, ?) ON DUPLICATE KEY UPDATE Supplier_ID=Supplier_ID");
            for (masterData.Product product : masterData.md_buffer_pdt) {
                pstmtSupplier.setInt(1, product.supplierId);
                pstmtSupplier.setString(2, product.supplierName);
                pstmtSupplier.addBatch();
            }
            pstmtSupplier.executeBatch();

            // Load Store Dimension
            PreparedStatement pstmtStore = c.prepareStatement(
                    "INSERT INTO Store (Store_ID, Store_Name) VALUES (?, ?) ON DUPLICATE KEY UPDATE Store_ID=Store_ID");
            for (masterData.Product product : masterData.md_buffer_pdt) {
                pstmtStore.setInt(1, product.storeId);
                pstmtStore.setString(2, product.storeName);
                pstmtStore.addBatch();
            }
            pstmtStore.executeBatch();

            // Load Time Dimension
            PreparedStatement pstmtTime = c.prepareStatement(
                    "INSERT INTO Time_Dimension (Time_ID, Transaction_Date, Weekend, Time, Half_Of_Year, Month, Quarter, Year) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE Time_ID=Time_ID");
            for (transdata transaction : Hashtable.values()) {
                pstmtTime.setInt(1, transaction.time_id);
                String[] dateTimeParts = transaction.order_date.split(" ");
                String[] dateParts = dateTimeParts[0].split("/");
                String formattedDate = dateParts[2] + "-" + dateParts[0] + "-" + dateParts[1];
                pstmtTime.setString(2, formattedDate);
                
                // Extract time and calculate additional fields
                String timePart = dateTimeParts[1];
                pstmtTime.setTime(4, Time.valueOf(timePart + ":00"));
                
                java.util.Date parsedDate;
                try {
                    parsedDate = new SimpleDateFormat("yyyy-MM-dd").parse(formattedDate);
                } catch (java.text.ParseException e) {
                    e.printStackTrace();
                    continue;
                }
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(parsedDate);
                
                int weekend = (Math.random() < 0.5) ? 0 : 1;
                pstmtTime.setInt(3, weekend);
                
                int month = calendar.get(Calendar.MONTH) + 1;
                pstmtTime.setInt(5, (month <= 6) ? 1 : 2);
                pstmtTime.setInt(6, month);
                pstmtTime.setInt(7, (month - 1) / 3 + 1);
                pstmtTime.setInt(8, calendar.get(Calendar.YEAR));
                
                pstmtTime.addBatch();
            }
            pstmtTime.executeBatch();

            // Load Sales Fact Table with logging
            PreparedStatement pstmtSalesInsert = c.prepareStatement(
                    "INSERT INTO Sales (Order_ID, Product_ID, Customer_ID, Supplier_ID, Time_ID, Store_ID, Quantity, Total_Sale) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            PreparedStatement pstmtSalesUpdate = c.prepareStatement(
                    "UPDATE Sales SET Quantity = ?, Total_Sale = ? WHERE Order_ID = ? AND Product_ID = ? AND Customer_ID = ?"
            );

            int insertCount = 0;
            int updateCount = 0;
            int failedInserts = 0;

            for (transdata transaction : Hashtable.values()) {
                try {
                    pstmtSalesInsert.setInt(1, transaction.transaction_id);
                    pstmtSalesInsert.setInt(2, transaction.product_id);
                    pstmtSalesInsert.setInt(3, transaction.customer_id);
                    pstmtSalesInsert.setInt(4, transaction.supplier_id);
                    pstmtSalesInsert.setInt(5, transaction.time_id);
                    pstmtSalesInsert.setInt(6, transaction.store_id);
                    pstmtSalesInsert.setInt(7, transaction.quantity);
                    pstmtSalesInsert.setDouble(8, transaction.sale);
                    pstmtSalesInsert.executeUpdate();
                    insertCount++;
                    
                } catch (SQLException e) {
                    failedInserts++;
                    
                    
                    // If the insert fails due to a duplicate key, attempt an update instead
                    try {
                        pstmtSalesUpdate.setInt(1, transaction.quantity);
                        pstmtSalesUpdate.setDouble(2, transaction.sale);
                        pstmtSalesUpdate.setInt(3, transaction.transaction_id);
                        pstmtSalesUpdate.setInt(4, transaction.product_id);
                        pstmtSalesUpdate.setInt(5, transaction.customer_id);
                        pstmtSalesUpdate.executeUpdate();
                        updateCount++;
                        
                    } catch (SQLException updateException) {
                        
                        
                    }
                }
            }

            System.out.println("Total inserts: " + insertCount);
            System.out.println("Total updates: " + updateCount);
            System.out.println("Total failed inserts: " + failedInserts);

            System.out.println("Data loaded into the data warehouse successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}