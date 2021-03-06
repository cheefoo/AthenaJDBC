package com.tayo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by temitayo on 3/14/17.
 */
public class AthenaJDBCWithSessionToken
{
    static final String athenaUrl = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443";
    private static final String ROLE_ARN = "arn:aws:iam::1111111111111:role/AthenaRole";

    public static void main(String[] args)
    {

        AWSCredentials longTermCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();

        AWSSecurityTokenServiceClient securityTokenServiceClient = new AWSSecurityTokenServiceClient(longTermCredentials);

        AssumeRoleRequest assumeRequest = new AssumeRoleRequest().withRoleArn(ROLE_ARN).withDurationSeconds(3600).withRoleSessionName("Test");

        AssumeRoleResult assumeResult = securityTokenServiceClient.assumeRole(assumeRequest);

        String myAccessKey = assumeResult.getCredentials().getAccessKeyId();
        String mySecretKey = assumeResult.getCredentials().getSecretAccessKey();
        String myToken = assumeResult.getCredentials().getSessionToken();
        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            Properties info = new Properties();
            info.put("s3_staging_dir", "s3://xxxxxxx/");
            info.put("log_path", "/Users/xxxxxxx/workspace/AthenaJDBC/log/xxxxx.log");
            info.put("aws_credentials_provider_class","com.tayo.CustomSessionCredentialsProvider");
            //info.put("aws_credentials_provider_arguments","credentials");
            String providerArgs = myAccessKey+"," + mySecretKey+"," + myToken;
            info.put("aws_credentials_provider_arguments", providerArgs);

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
