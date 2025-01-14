package ETL;



import java.sql.*;
import java.io.*;


public class trasactions_uploder {

    
    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/data_store"; 
        String username = "root";  // Replace with your DB username
        String password = "1234";  // Replace with your DB password
        String csvFile = "C:\\Users\\HP\\Desktop\\Data_Warehouse_Project\\transactions_data.csv";  

        

                Connection conn = null;
                PreparedStatement pstmt = null;

                String sql = "INSERT INTO TRANSACTIONS (Order_ID, Transaction_Date, Product_ID, Quantity, Customer_ID) " +
                             "VALUES (?, ?, ?, ?, ?)";

                try {
                    // Establish connection to the database
                    conn = DriverManager.getConnection(jdbcURL, username, password);

                    // Create a PreparedStatement
                    pstmt = conn.prepareStatement(sql);

                    // Read the CSV file
                    BufferedReader lineReader = new BufferedReader(new FileReader(csvFile));
                    String lineText;

                    // Skip the header line
                    lineReader.readLine();

                    while ((lineText = lineReader.readLine()) != null) {
                        String[] data = lineText.split(",");

                        // Parse the CSV values
                        int orderId = Integer.parseInt(data[0]);
                        String transactionDate = data[1]; // Keep as string since it's varchar in the table
                        int productId = Integer.parseInt(data[2]);
                        int quantity = Integer.parseInt(data[3]);
                        int customerId = Integer.parseInt(data[4]);

                        // Set the values in the PreparedStatement
                        pstmt.setInt(1, orderId);
                        pstmt.setString(2, transactionDate);
                        pstmt.setInt(3, productId);
                        pstmt.setInt(4, quantity);
                        pstmt.setInt(5, customerId);

                        // Add to batch
                        pstmt.addBatch();
                    }

                    // Execute the batch
                    int[] affectedRows = pstmt.executeBatch();
                    System.out.println("Inserted " + affectedRows.length + " rows into TRANSACTIONS table.");

                    // Close the file reader
                    lineReader.close();

                } catch (SQLException ex) {
                    ex.printStackTrace();
                } catch (IOException ex) {
                    ex.printStackTrace();
                } finally {
                    try {
                        if (pstmt != null) pstmt.close();
                        if (conn != null) conn.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
