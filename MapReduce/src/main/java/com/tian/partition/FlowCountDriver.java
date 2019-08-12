package com.tian.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "e:/git/hadoop/mapreduce/input/phone_data.txt", 
		"e:/git/hadoop/mapreduce/output" };
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowCountDriver.class);
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);

		job.waitForCompletion(true);
	}

}
