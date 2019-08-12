package com.tian.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.deser.std.StringDeserializer;

import java.util.*;

/**
 * @author Friday
 * @date 2019/8/12 23:31
 */
public class CustomOffset {
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tian");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            //该方法会在Rebalance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            //该方法会在Rebalance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉去数据
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset:%d, key:%s, value:%s$n",
                            record.offset(), record.key(), record.value());
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
                }
                commitOffset(currentOffset);
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 获取分区的最新offset
     * @param partition
     * @return
     */
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    /**
     * 提交该消费者所有分区的offset
     * @param currentOffset
     */
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
