package com.tian.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 同步手动提交offset
 *
 * @author JARVIS
 * @date 2019/8/12 21:07
 */
public class SyncManualCommitOffset {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092"); //kafka集群
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tian"); //消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); //关闭自动提交offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()); //key反序列化，
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first")); //消费者订阅主题，可添加多个
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);//拉取数据
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("offset:" + record.offset() +
                            "key:" + record.key() + "value:" + record.value());
                    consumer.commitSync(); //同步提交，当前线程会阻塞直到offset提交成功
                }
            }
        } finally {
            consumer.close();
        }
    }
}
