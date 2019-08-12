package com.tian.indexinvert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexInvertDriver1 {

	// 第一次处理
	// 第二次处理之前直接调用该方法，传值为第一次处理的输入输出路径
	public static void job(String[] path) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(IndexInvertDriver1.class);
		job.setMapperClass(IndexInvertMapper1.class);
		job.setReducerClass(IndexInvertReducer1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(path[0]));
		FileOutputFormat.setOutputPath(job, new Path(path[1]));
		
		job.waitForCompletion(true);
	}
}
