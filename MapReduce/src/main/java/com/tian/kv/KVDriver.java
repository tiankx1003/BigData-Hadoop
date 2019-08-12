package com.tian.kv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVDriver {
	public static void main(String[] args) throws Exception {

		args = new String[] { "e:/git/hadoop/mapreduce/input/KV.txt", 
				"e:/git/hadoop/mapreduce/output3/" };// 输入输出路径

		// 1.获取job
		Configuration conf = new Configuration();

		// 设置切割符
		// org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);

		// 2.关联jar
		job.setJarByClass(KVDriver.class);

		// 3.关联mapper和reducer
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);

		// 4.设置Mapper的输出key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5.设置最终输出key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6.设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 设置输入格式
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 7.提交job
		job.waitForCompletion(true);
	}

}
