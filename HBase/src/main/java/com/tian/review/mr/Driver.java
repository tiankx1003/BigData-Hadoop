package com.tian.review.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * 将fruit中数据的一部分通过mr迁移到fruit_mr中
 * @author Friday
 * @date 2019/8/15 21:52
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hadoop101.quorum","hadoop101,hadoop102,hadoop103");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("fruit",scan,ReadMapper.class,
                ImmutableBytesWritable.class,
                Put.class,job);
        job.setNumReduceTasks(10);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",WriteReducer.class,
                job, HRegionPartitioner.class);
        job.waitForCompletion(true);
    }
}
