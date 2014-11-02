package main.java;

import java.sql.*;

public class A3 {
    // The JDBC Connector Class.
	
    private static final String dbClassName = "com.mysql.jdbc.Driver";
    // Connection string. cs348 is the database the program is trying to connection
    // is connecting to,127.0.0.1 is the local loopback IP address for this machine, user name for the connection 
    // is root, password is cs348
    // private static final String CONNECTION_STRING = "jdbc:mysql://127.0.0.1/tpch?user=root&password=cs348&Database=tpch;";
    
    
    public static void main(String[] args) throws ClassNotFoundException,SQLException{
    	String CONNECTION_STRING = args[0];
    	String inputFile = args[1];
    	
    	// Try to connect
        Connection con = DriverManager.getConnection(CONNECTION_STRING);
        System.out.println("Connection Established");
        
        String query = "SELECT COUNT(*) FROM LINEITEM AS CNT";
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        
        con.close();
    }
    
    
}
