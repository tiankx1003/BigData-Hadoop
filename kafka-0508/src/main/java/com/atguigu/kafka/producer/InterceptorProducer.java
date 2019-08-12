package com.atguigu.kafka.producer;

import com.atguigu.kafka.interceptor.CountInterceptor;
import com.atguigu.kafka.interceptor.TimeInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class InterceptorProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                TimeInterceptor.class.getName()
                + "," + CountInterceptor.class.getName());

        //1.创建KafkaProducer对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //2.调用send方法
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first", "message-" + i);
            RecordMetadata metadata = producer.send(record).get();
            if (metadata != null) {
                System.out.println("success:" + metadata.topic() + "-"
                        + metadata.partition() + "-" + metadata.offset());
            }
        }

        producer.close();
    }
}
