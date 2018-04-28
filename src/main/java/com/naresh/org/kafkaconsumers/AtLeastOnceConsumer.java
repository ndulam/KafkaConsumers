package com.naresh.org.kafkaconsumers;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AtLeastOnceConsumer {
    public static void main(String[] str) throws InterruptedException {
            System.out.println("Starting AutoOffsetGuranteedAtLeastOnceConsumer ...");
            execute();
     }
    private static void execute() throws InterruptedException {
            KafkaConsumer<String, String> consumer = createConsumer();
            // Subscribe to all partition in that topic. 'assign' could be used here
            // instead of 'subscribe' to subscribe to specific partition.
            consumer.subscribe(Arrays.asList("normal-topic"));
            processRecords(consumer);
     }
     private static KafkaConsumer<String, String> createConsumer() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            String consumeGroup = "cg1";
            props.put("group.id", consumeGroup);
            // Set this property, if auto commit should happen.
            props.put("enable.auto.commit", "true");
            // Make Auto commit interval to a big number so that auto commit does not happen,
            // we are going to control the offset commit via consumer.commitSync(); after processing             // message.
            props.put("auto.commit.interval.ms", "999999999999");
            // This is how to control number of messages being read in each poll
            props.put("max.partition.fetch.bytes", "35");
            props.put("heartbeat.interval.ms", "3000");
            props.put("session.timeout.ms", "6001");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            return new KafkaConsumer<String, String>(props);
    }
     private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException  {
            while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    long lastOffset = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(),                                         record.key(), record.value());
                        lastOffset = record.offset();
                    }
                    System.out.println("lastOffset read: " + lastOffset);
                    process();
                    // Below call is important to control the offset commit. Do this call after you
                    // finish processing the business process.
                    consumer.commitSync();
            }
    }
    private static void process() throws InterruptedException {
        // create some delay to simulate processing of the record.
        Thread.sleep(20);
    }
}