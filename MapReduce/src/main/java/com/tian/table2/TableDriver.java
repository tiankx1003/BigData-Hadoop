package com.tian.table2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {
	public static void main(String[] args) throws Exception {
		
		/*
		 * 设置路径
		 */
		args = new String[] { "path1", "path2" };

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(TableDriver.class);

		job.setMapperClass(TableMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * 添加缓存文件
		 */
		job.addCacheFile(new URI("cacheFile"));

		job.setNumReduceTasks(0);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
