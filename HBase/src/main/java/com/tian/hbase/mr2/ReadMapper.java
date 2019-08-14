package com.tian.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 16:29
 */
public class ReadMapper extends Mapper<Long, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(Long key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");//split方法传入的参量是一个正则表达式
        if (split.length<3)
            return;
        Put put = new Put(Bytes.toBytes(split[0])); //split[0]即rowKey
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(split[2]));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])),put);
    }
}
