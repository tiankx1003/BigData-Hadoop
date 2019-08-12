package com.tian.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MySink extends AbstractSink implements Configurable {
    private Integer batchSize = null;
    private String prifix = null;
    private List<Event> events = new ArrayList<>();
    private static Logger LOG = LoggerFactory.getLogger(MySink.class);

    @Override
    public Status process() throws EventDeliveryException {
        events.clear();//使用之前先清空events
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin(); //开启事务

        try {
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) break; //event为空直接跳出循环
                events.add(event);
            }
        } catch (Exception e) {
            //捕捉到异常后回滚
            txn.rollback();
            return Status.BACKOFF;
        } finally {
            txn.close(); //关闭事务
        }

        return Status.READY;
    }

    /**
     * 从配置文件获取参数
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        batchSize = context.getInteger("batchSize");
        prifix = context.getString("prefix");
    }
}
