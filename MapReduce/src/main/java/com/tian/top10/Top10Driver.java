package com.tian.top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10Driver {

	public static void main(String[] args) throws Exception {

		//TODO 添加输入输出路径
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(Top10Mapper.class);
		job.setReducerClass(Top10Reducer.class);
		job.setJarByClass(Top10Driver.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
