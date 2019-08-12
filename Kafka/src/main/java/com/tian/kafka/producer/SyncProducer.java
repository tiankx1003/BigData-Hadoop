package com.tian.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 同步Producer API
 */
public class SyncProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //创建properties对象用于存放配置
        Properties props = new Properties();
        //添加配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1); //重试次数
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        //通过已有配置创建kafkaProducer对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //循环调用send方法不断发送数据
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("first", "message" + i);
            RecordMetadata metadata = producer.send(record).get();//通过get()方法实现同步效果
            if (metadata != null)
                System.out.println("success:" + metadata.topic() + "-" +
                        metadata.partition() + "-" + metadata.offset());
        }
        producer.close(); //关闭生产者对象
    }
}
