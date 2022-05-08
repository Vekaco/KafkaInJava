package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.SimpleTimeZone;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("acks", "all");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i =0; i < 300; i++) {
            producer.send(new ProducerRecord<>("quickstart-events", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + Integer.toString(i), "value" + i));
        }
        producer.close();
    }
}
