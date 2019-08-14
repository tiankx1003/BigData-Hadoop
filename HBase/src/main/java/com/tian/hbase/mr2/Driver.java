package com.tian.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 16:38
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hadoop.zookeeper.quorum",
                "hadoop101,hadoop102,hadoop103");
        Job job = Job.getInstance(conf);
        job.setJarByClass(com.tian.hbase.mr.Driver.class);
        job.setMapperClass(ReadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                WriteReducer.class,job);
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
//        if (!isSuccess)
//            throw new IOException("Job running with error");
//        return isSuccess ? 0:1;
    }
}
