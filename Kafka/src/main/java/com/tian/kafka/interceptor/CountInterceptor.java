package com.tian.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 统计发送消息成功和发送消息失败数，
 * 并在producer关闭时打印这连个计时器
 * @author Friday
 * @date 2019/8/13 0:14
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    /**
     * 直接返回传入的参量
     * @param producerRecord
     * @return producerRecord
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    /**
     * 统计成功和失败的次数
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null)
            successCounter++;
        else
            errorCounter++;
    }

    /**
     * 打印结果
     */
    @Override
    public void close() {
        System.out.println("Successful sent:" + successCounter);
        System.out.println("Failed sent:" + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
