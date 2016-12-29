package com.kafka;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import kafka.producer.ProducerConfig;
import kafka.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by speng on 2016/12/26.
 * Kafka&Zookeeper configuration
 */
public class ApplicationConfiguration {
    /**
     * KAFKA & ZOOKEEPER.
     */
    private static final String BROKER_LIST = "metadata.broker.list";
    private static final String TOPIC_NAME = "topic-name";
    private static final String CONSUMER_GROUP_ID = "group-id";
    private static final String ZOOKEEPER_CONNECTION = "zookeeper-connection";
    private static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper-session-timeout";
    private static final String ZOOKEEPER_SYNC_TIME_INTERVAL = "zookeeper-sync-time";


    private static final String PROPERTIES = "application.properties";
    private static Configuration appConfiguration;

    public ApplicationConfiguration() throws Exception{
        setAppConfiguration(new PropertiesConfiguration(PROPERTIES));
    }

    private static void setAppConfiguration(Configuration appConfiguration) {
        ApplicationConfiguration.appConfiguration = appConfiguration;
    }

    public ProducerConfig getProducerConfig(){
        Properties props = new Properties();
        props.put(BROKER_LIST, appConfiguration.getString(BROKER_LIST));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        return new ProducerConfig(props);
    }

    public ConsumerConfig getConsumerConfig(){
        Properties props = new Properties();
        props.put("zookeeper.connect", appConfiguration.getString(ZOOKEEPER_CONNECTION));
        props.put("group.id", appConfiguration.getString(CONSUMER_GROUP_ID));
        props.put("zookeeper.session.timeout.ms", appConfiguration.getString(ZOOKEEPER_SESSION_TIMEOUT));
        props.put("zookeeper.sync.time.ms", appConfiguration.getString(ZOOKEEPER_SYNC_TIME_INTERVAL));
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public String getTopicName(){
        return appConfiguration.getString(TOPIC_NAME);
    }
}
