package com.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NonBlockingProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        // this is critical
        // props.setProperty("transactional.id", "my-transactional-id");
        Producer<byte[], byte[]> producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());

        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("quickstart-events", key, value);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e!=null) {
                    e.printStackTrace();
                } else {
                    System.out.println("the offset of the record we just sent is " + recordMetadata.offset());
                }
            }
        });
        List<PartitionInfo> partitions = producer.partitionsFor("quickstart-events");
        for (PartitionInfo partition: partitions) {
            System.out.println(partition.partition());
        }
        Map<MetricName, ? extends Metric> metrics = producer.metrics();

        producer.flush();
        producer.close();
    }
}
