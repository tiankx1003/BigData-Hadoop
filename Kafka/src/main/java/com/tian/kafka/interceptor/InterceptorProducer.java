package com.tian.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ser.std.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 主程序
 * @author Friday
 * @date 2019/8/13 0:21
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.RETRIES_CONFIG,0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33664432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        //构建拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.tian.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.tian.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        String topic = "first";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //发送消息
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + 1);
            producer.send(record);
        }
        //关闭producer
        producer.close();
    }
}