package com.naresh.org.kafkaconsumers;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

public class MyConsumerRebalancerListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    private OffsetManager offsetManager = new OffsetManager("storage2");
    private Consumer<String, String> consumer;

    public MyConsumerRebalancerListener(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        for (TopicPartition partition : partitions) {

            offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {


        for (TopicPartition partition : partitions) {
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
    }


}