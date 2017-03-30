package com.tayo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

/**
 * Created by temitayo on 3/14/17.
 */
public class CustomSessionCredentialsProvider implements com.amazonaws.auth.AWSCredentialsProvider
{
    private  AWSCredentials credentials;

    public CustomSessionCredentialsProvider(String accessId, String secretKey, String token)
    {
       credentials = new BasicSessionCredentials(accessId, secretKey, token);
    }



    public AWSCredentials getCredentials()
    {
        return credentials;
    }

    public void refresh()
    {

    }
}
