package com.tian.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;

import java.util.ArrayList;
import java.util.List;

public class MyPollalbeSource extends AbstractPollableSource {
    private String prefix = null; //前缀
    private Integer batchSize = null; //用于判断个数
    private List<Event> events = new ArrayList<>(); //用于用于封装多个event

    @Override
    protected Status doProcess() throws EventDeliveryException {
        //可以在方法的最开始就清空events集合
        try {
            //读取数据并封装成event
            for (int i = 0; i < batchSize; i++) {
                String message = prefix + "-" + i;
                Event event = EventBuilder.withBody(message.getBytes());
                events.add(event);
            }
            //events交给ChannelProcessor
            ChannelProcessor channelProcessor = getChannelProcessor();
            channelProcessor.processEventBatch(events);
            events.clear(); //每次处理之后清空集合
        } catch (Exception e) {
            events.clear(); //发生异常也要清空集合
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        prefix = context.getString("prefix");
        batchSize = context.getInteger("batchSize", 20);
    }

    /**
     * 开始时打印提示
     * @throws FlumeException
     */
    @Override
    protected void doStart() throws FlumeException {
        System.out.println("******************************");
        System.out.println("my custom source start...");
        System.out.println("******************************");
    }

    /**
     * 结束时打印提示
     * @throws FlumeException
     */
    @Override
    protected void doStop() throws FlumeException {
        System.out.println("******************************");
        System.out.println("my custom source stop...");
        System.out.println("******************************");
    }
}
