package com.tian.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * 以端口数据模拟日志，以数字（单个）和字母（单个）模拟不同类型的日志，
 * 自定义interceptor区分数字和字母，
 * 将其分别发往不同的分析系统（Channel）。
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 业务逻辑:
     * 根据event内容给header的键值对赋值
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if (body[0] >= '1' && body[0] <= '9') {
            event.getHeaders().put("type", "number");
        } else if (body[0] >= 'a' && body[0] <= 'z') {
            event.getHeaders().put("type", "letter");
        }
        return event;
    }

    /**
     * 循环调用intercept(Event event)方法
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        /**
         * 返回一个外部类对象
         *
         * @return
         */
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
