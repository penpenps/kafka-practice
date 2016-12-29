package com.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.logging.Logger;

/**
 * Created by speng on 2016/12/26.
 * Demo for message producer
 */
public class SimpleProducer {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    private static Producer<String, String> producer;
    private static ApplicationConfiguration config;
    public SimpleProducer() throws Exception{
        config = new ApplicationConfiguration();
        producer = new Producer<String, String>(config.getProducerConfig());
    }

    public void sendMessage(String msg){
        LOGGER.info("Send message [" + msg + "]");
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(config.getTopicName(), msg);
        producer.send(data);
    }

    public void close(){
        producer.close();
    }

    public static void main(String[] args) throws Exception{
        SimpleProducer producer = new SimpleProducer();
        long events = 1000;
        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String msg = "Message on " + runtime;
            producer.sendMessage(msg);
        }
        producer.close();
    }
}
