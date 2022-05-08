package com.example.kafka.consumer;

import com.sun.xml.internal.ws.addressing.WsaTubeHelperImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class SpecifyOffsetConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-consumer-group");
        //disable auto commit
        props.setProperty("enable.auto.commit", "false");
        // read committed messages only to ensure the transactional messages reading
        props.setProperty("isolation.level", "read_committed");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer consumer = new KafkaConsumer(props);

        String topic = "quickstart-events";
        //want to bind to partition 0
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> topics = Arrays.asList(topicPartition);
        consumer.assign(topics);
        //consumer.seekToBeginning(topics);
        //consuming the latest 1000 messages
        consumer.seekToEnd(topics);
        long current = consumer.position(topicPartition);
        consumer.seek(topicPartition, current-1000);


       // int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//                count++;
//                if (count % 100 ==0) {
//                    consumer.pause(Arrays.asList(topicPartition));
//                    Thread.sleep(1000);
//                    consumer.resume(Arrays.asList(topicPartition));
//                }

            }
        }

    }
}
