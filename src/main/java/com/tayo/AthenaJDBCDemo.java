package com.tayo;

import java.sql.*;
import java.util.Properties;
import com.amazonaws.athena.jdbc.AthenaDriver;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;


public class AthenaJDBCDemo {

    static final String athenaUrl = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443";

    public static void main(String[] args) {

        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            Properties info = new Properties();
            info.put("s3_staging_dir", "s3://xxxx/");
            info.put("log_path", "/Users/xxxxx/workspace/AthenaJDBC/log/athenajdbc.log");
            /*info.put("aws_credentials_provider_class","com.amazonaws.auth.PropertiesFileCredentialsProvider");
            info.put("aws_credentials_provider_arguments","credentials");*/
            info.put("aws_credentials_provider_class","com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
            //info.put("aws_credentials_provider_arguments","credentials");



            String databaseName = "default";

            System.out.println("Connecting to Athena...");
            conn = DriverManager.getConnection(athenaUrl, info);

            System.out.println("Listing tables...");
            String sql = "show tables in "+ databaseName;
            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                //Retrieve table column.
                String name = rs.getString("tab_name");

                //Display values.
                System.out.println("Name: " + name);
            }
            rs.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (Exception ex) {

            }
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception ex) {

                ex.printStackTrace();
            }
        }
        System.out.printf("Finished connectivity test.");
    }
}
