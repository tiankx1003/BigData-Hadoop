package com.tian.myinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * debug
 */
public class MyInputFormatDriver {

	public static void main(String[] args) throws Exception {

		args = new String[] { "e:/git/hadoop/MapReduce/input/myinputformat",
				"e:/git/hadoop/MapReduce/output" };

		// 1.获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.关联jar包 mapper reducer
		job.setJarByClass(MyInputFormatDriver.class);
		job.setMapperClass(MyInputFormatMapper.class);
		job.setReducerClass(MyInputFormatReducer.class);

		// 7.设置输入的inputFormat
		job.setInputFormatClass(MyInputFormat.class);
		// 8.设置输出的outputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 3.设置mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		// 4.设置最终输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		// 5.设置路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6.提交job
		job.waitForCompletion(true);
	}
}
