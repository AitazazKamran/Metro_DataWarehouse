package ETL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.io.BufferedReader;
import java.io.FileReader;

public class master_data_upload {

    // Database credentials
    private static final String DB_URL = "jdbc:mysql://localhost:3306/Master_Data";
    private static final String USER = "root"; // Replace with your username
    private static final String PASS = "1234"; // Replace with your password

    public static void main(String[] args) {
        // Paths to the CSV files
        String productsCsvFile = "C:\\Users\\HP\\Desktop\\Data_Warehouse_Project\\products_data.csv";  // Replace with your CSV file path
        String customersCsvFile = "C:\\Users\\HP\\Desktop\\Data_Warehouse_Project\\customers_data.csv"; // Replace with your customers CSV file path
        
        String line;
        String csvSplitBy = ",";
        
        // SQL insert statements for products and customers
        String insertProductsQuery = "INSERT INTO products (Product_ID, Product_Name, Price, Supplier_ID, Supplier_Name, Store_ID, Store_Name) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        String insertCustomersQuery = "INSERT IGNORE INTO customers (Customer_ID, Customer_Name, Gender) "
                + "VALUES (?, ?, ?)";

        
        try (BufferedReader brProducts = new BufferedReader(new FileReader(productsCsvFile));
             BufferedReader brCustomers = new BufferedReader(new FileReader(customersCsvFile));
             Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             PreparedStatement stmtProducts = conn.prepareStatement(insertProductsQuery);
             PreparedStatement stmtCustomers = conn.prepareStatement(insertCustomersQuery)) {

            // Skip the header lines (optional)
            brProducts.readLine(); 
            brCustomers.readLine(); 

            // Process products CSV
            while ((line = brProducts.readLine()) != null) {
                String[] values = line.split(csvSplitBy);

                // Product ID, Supplier ID, Store ID should be integers, handle any conversion errors
                try {
                    stmtProducts.setInt(1, Integer.parseInt(values[0])); // Product_ID
                    stmtProducts.setString(2, values[1]);                // Product_Name
                    stmtProducts.setDouble(3, Double.parseDouble(values[2].replace("$", ""))); // Price
                    stmtProducts.setInt(4, Integer.parseInt(values[3])); // Supplier_ID
                    stmtProducts.setString(5, values[4]);                // Supplier_Name
                    stmtProducts.setInt(6, Integer.parseInt(values[5])); // Store_ID
                    stmtProducts.setString(7, values[6]);                // Store_Name
                    
                    stmtProducts.addBatch();  // Add to batch
                } catch (NumberFormatException e) {
                    System.out.println("Skipping invalid product data: " + line);
                }
            }
            stmtProducts.executeBatch();  // Execute the batch insert for products
            System.out.println("Product data uploaded successfully!");

            // Process customers CSV
            while ((line = brCustomers.readLine()) != null) {
                String[] values = line.split(csvSplitBy);
                
                // Customer ID should be an integer, handle any conversion errors
                try {
                    stmtCustomers.setInt(1, Integer.parseInt(values[0])); // Customer_ID
                    stmtCustomers.setString(2, values[1]);               // Customer_Name
                    stmtCustomers.setString(3, values[2]);               // Gender
                    
                    stmtCustomers.addBatch();  // Add to batch
                } catch (NumberFormatException e) {
                    System.out.println("Skipping invalid customer data: " + line);
                }
            }
            stmtCustomers.executeBatch();  // Execute the batch insert for customers
            System.out.println("Customer data uploaded successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
