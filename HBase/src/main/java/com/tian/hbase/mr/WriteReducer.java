package com.tian.hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 15:30
 */
public class WriteReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    /**
     * 归并相同的rowKey后直接写出
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
