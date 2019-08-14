package com.tian.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * TODO 结果验证
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 15:30
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hadoop.zookeeper.quorum",
                "hadoop101,hadoop102,hadoop103");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        /*
        使用HBase工具类初始化
         */
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("fruit",scan,ReadMapper.class,
                ImmutableBytesWritable.class,
                Put.class,job);
        job.setNumReduceTasks(100);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",WriteReducer.class,
                job, HRegionPartitioner.class); //TODO 源码
        job.waitForCompletion(true);
    }
}
