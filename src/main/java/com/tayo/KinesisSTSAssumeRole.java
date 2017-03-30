package com.tayo;

import com.amazonaws.auth.AWSCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;



import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;



/**
 * Created by temitayo on 2/2/17.
 */
public class KinesisSTSAssumeRole
{


    final static String STREAM_NAME = "tayostream";

    //private static final String ROLE_ARN = "arn:aws:iam::573906581002:role/KinesisAssumeReadRole";

    private static final String ROLE_ARN = "arn:aws:iam::295713827608:role/kinesistayorole";

    public static void main (String [] args) throws IOException
    {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("AwsCreds.properties");

        //AWSCredentials longTermCredentials = new PropertiesCredentials(input);

        AWSCredentials longTermCredentials = new DefaultAWSCredentialsProviderChain().getCredentials();

        AWSSecurityTokenServiceClient securityTokenServiceClient = new AWSSecurityTokenServiceClient(longTermCredentials);

        AssumeRoleRequest assumeRequest = new AssumeRoleRequest().withRoleArn(ROLE_ARN).withDurationSeconds(3600).withRoleSessionName("Test");

        AssumeRoleResult assumeResult = securityTokenServiceClient.assumeRole(assumeRequest);

        System.out.println("Access Key is " + assumeResult.getCredentials().getAccessKeyId());


        AWSCredentials temporaryCredentials;

        temporaryCredentials = new BasicSessionCredentials
                (assumeResult.getCredentials().getAccessKeyId(), assumeResult.getCredentials().getSecretAccessKey(), assumeResult.getCredentials().getSessionToken());



        AmazonKinesisClient kinesis = new AmazonKinesisClient(temporaryCredentials);
        System.out.println("Client obtained successfully");
        kinesis.setEndpoint("kinesis.us-east-1.amazonaws.com");

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(STREAM_NAME);

        DescribeStreamResult result = kinesis.describeStream(describeStreamRequest);

        System.out.println(result.toString());

        for(int i =0; i < 100; i++)
        {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(STREAM_NAME);
            putRecordRequest.setData(ByteBuffer.wrap(String.format("TestData").getBytes("UTF-8")));
            putRecordRequest.setPartitionKey("Test");
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            System.out.println(i+ "with seq number" +  putRecordResult.getSequenceNumber());
        }

        System.out.println(result.toString());
        java.util.List<Shard> shardList = result.getStreamDescription().getShards();

        java.util.List<Record> records;
        for(Shard shard: shardList)
        {
            String shardIterator = null;
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(STREAM_NAME);
            getShardIteratorRequest.setShardId(shard.getShardId());
            getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
            GetShardIteratorResult gsr = kinesis.getShardIterator(getShardIteratorRequest);
            shardIterator = gsr.getShardIterator();

            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(50);

            GetRecordsResult getRecordsResult = kinesis.getRecords(getRecordsRequest);

            records = getRecordsResult.getRecords();
            if(records.size()>1)
            {
                System.out.println("Received " + records.size() + " records from shard " + shard);

            }
            else
            {
                System.out.println("Received " + records.size() + " records from shard " + shard);
            }


        }


    }
}
