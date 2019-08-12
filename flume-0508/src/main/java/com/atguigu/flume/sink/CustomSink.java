package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CustomSink extends AbstractSink implements Configurable {

    private Integer batchSize = null;
    private String prefix = null;
    private List<Event> events = new ArrayList<>();
    private static Logger LOG = LoggerFactory.getLogger(CustomSink.class);

    @Override
    public Status process() throws EventDeliveryException {

        events.clear();
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();

        tx.begin();

        try {
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) break;
                events.add(event);
            }

            for (Event event : events) {
                LOG.info(prefix + "-" + new String(event.getBody()));
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            return Status.BACKOFF;
        } finally {
            tx.close();
        }

        return Status.READY;
    }

    @Override
    public void configure(Context context) {
        batchSize = context.getInteger("batchSize");
        prefix = context.getString("prefix");
    }
}
