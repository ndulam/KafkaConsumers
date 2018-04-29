package com.naresh.org.kafkaconsumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ExactlyOnceDynamicConsumer {

    private static OffsetManager offsetManager = new OffsetManager("storage2");

    public static void main(String[] str) throws InterruptedException {

        System.out.println("Starting ManualOffsetGuaranteedExactlyOnceReadingDynamicallyBalancedPartitionConsumer ...");

        readMessages(str[0]);

    }

    /**
     */
    private static void readMessages(String topic) throws InterruptedException {

        KafkaConsumer<String, String> consumer = createConsumer();

        // Manually controlling offset but register consumer to topics to get dynamically assigned partitions.
        // Inside MyConsumerRebalancerListener use consumer.seek(topicPartition,offset) to control offset

        consumer.subscribe(Arrays.asList(topic), new MyConsumerRebalancerListener(consumer));

        processRecords(consumer);
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "quickstart.cloudera:9092,quickstart.cloudera:9093,quickstart.cloudera:9094");
        String consumeGroup = "cg3";

        props.put("group.id", consumeGroup);

        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");

        // Control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "40");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    private static void processRecords(KafkaConsumer<String, String> consumer) {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());

            }
        }
    }


}