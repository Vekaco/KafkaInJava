package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerRunner implements Runnable{
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

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

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);

        KafkaConsumerRunner runner = new KafkaConsumerRunner(kafkaConsumer);
        Thread thread = new Thread(runner);
        thread.start();

        KafkaConsumer kafkaConsumer2 = new KafkaConsumer(props);

        KafkaConsumerRunner runner2 = new KafkaConsumerRunner(kafkaConsumer2);
        Thread thread2 = new Thread(runner2);
        thread2.start();

//        Thread.sleep(1000);
//        runner.shutdown();
    }

    public KafkaConsumerRunner(KafkaConsumer consumer) {
        this.consumer = consumer;
    }
    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList("quickstart-events"));

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                //handle new records
                for (ConsumerRecord record : records) {
                    System.out.printf("current:%s, partition: %d, offset: %d, key=%s, value=%s\n",
                            Thread.currentThread().getName(), record.partition(), record.offset(), record.key(), record.value());
                }
            }

        } catch (WakeupException e) {
            //ignore exception if closing
            if (!closed.get()) throw e;

        } finally {
            consumer.close();
        }
    }

    //shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
