package com.twitter.data.utilities;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by jmoffitt on 11/16/14.
 */
public class Environment {

    private static final Logger logger = Logger.getLogger(Environment.class);
    private final Properties props;

    public Environment() {

        props = new Properties();
        InputStream properties = Environment.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            props.load(properties);
        } catch (IOException e) {
            logger.error("Could not load properties, streams cannot be configured");
            throw new RuntimeException("Could not load properties");
        }
    }

    //Streaming credentials.
    public String userName() {
        return String.valueOf(props.get("username"));
    }
    public String userPassword() {
        return String.valueOf(props.get("password"));
    }
    public String accountName() {
        return String.valueOf(props.get("account_name"));
    }
    public String streamLabel() {
        return String.valueOf(props.get("stream_label"));
    }
    public String productName() {
        return String.valueOf(props.get("product_name"));
    }

    //Datastore credentials.
    public String dsType() {
        return String.valueOf(props.get("datastore_type"));
    }
    public String dsHost() {
        return String.valueOf(props.get("datastore_host"));
    }
    public String dsUsername() {
        return String.valueOf(props.get("datastore_username"));
    }
    public String dsPassword() {
        return String.valueOf(props.get("datastore_password"));
    }
    public String dsPort() {
        return String.valueOf(props.get("datastore_port"));
    }
    public String dsDatabase() {return String.valueOf(props.get("datastore_database"));}
    public String dsBatchSize() {
        return String.valueOf(props.get("datastore_activity_batch_size"));
    }

    //Set methods.
    //Streaming credentials.
    public void setUserName(String userName) {props.setProperty("username",userName);}
    public void setPassword(String password) {props.setProperty("password",password);}
    public void setAccountName(String accountName) {props.setProperty("account_name",accountName);}
    public void setStreamLabel(String streamLabel) {props.setProperty("stream_label",streamLabel);}
    public void setProductName(String productName) {props.setProperty("product_name",productName);}
}