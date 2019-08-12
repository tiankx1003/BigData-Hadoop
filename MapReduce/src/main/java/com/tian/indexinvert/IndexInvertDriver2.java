package com.tian.indexinvert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 可以尝试再一次job中直接实现该需求
 * @author Friday
 *
 */
public class IndexInvertDriver2 {

	public static void main(String[] args) throws Exception {
		
		//invoke job1
		String[] path = {"path1","path2"}; //job1 path
		IndexInvertDriver1.job(path);
		
		args = new String[] {"path1","path2"}; //job2 path
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);

		job.setJarByClass(IndexInvertDriver2.class);
		job.setMapperClass(IndexInvertMapper2.class);
		job.setReducerClass(IndexInvertReducer2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		
	}
}
