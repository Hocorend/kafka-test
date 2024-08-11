package com.example.kafkatest.configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import static java.lang.System.*;

public class AppConfig {

    public static String KAFKA_CONSUMER_GROUP_ID;
    private static String ROOT_PATH;
    public static String KAFKA_PRODUCER_CLIENT_ID;
    public static String KAFKA_PRODUCER_KEY;
    public static String BOOTSTRAP_SERVERS;
    public static String configFile;

    static {
        ROOT_PATH = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("").getPath());

        configFile = getProperty("configFile");
        if (configFile == null || configFile.isEmpty()){
            configFile = "application.properties";
        }

        Properties properties = getProperties();
        KAFKA_PRODUCER_CLIENT_ID = properties.getProperty("kafka.producer.client.id");
        KAFKA_PRODUCER_KEY = properties.getProperty("kafka.producer.key");

        KAFKA_CONSUMER_GROUP_ID = properties.getProperty("kafka.consumer.group.id");

        BOOTSTRAP_SERVERS = properties.getProperty("bootstrap.servers");
    }

    private static Properties getProperties(){
        Properties properties = new Properties();
        try{
            properties.load(new FileInputStream(ROOT_PATH + configFile));
        } catch (IOException e ){
            e.printStackTrace();
        }
        return properties;
    }
}
