package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import sun.rmi.runtime.Log;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

public class CustomOffsetConsumer {

    private static HashMap<TopicPartition, Long> currentOffsets = new HashMap<>();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata-0508");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffsets.clear();
                for (TopicPartition partition : partitions) {
                    Long offset = getOffsetByTopicPartition(partition);
                    consumer.seek(partition, offset);
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                //tx.begin
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                }
                commitOffset(currentOffsets);
                //tx.commit
            }
        } finally {
            consumer.close();
        }
    }

    private static void commitOffset(HashMap<TopicPartition, Long> currentOffsets) {

    }

    private static Long getOffsetByTopicPartition(TopicPartition partition) {
        return null;
    }
}
