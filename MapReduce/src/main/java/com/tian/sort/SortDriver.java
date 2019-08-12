package com.tian.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "e:/git/hadoop/mapreduce/input/phone_data.txt", 
		"e:/git/hadoop/mapreduce/output2" };
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(SortDriver.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setPartitionerClass(SortPartitioner.class);
		job.setNumReduceTasks(5);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
