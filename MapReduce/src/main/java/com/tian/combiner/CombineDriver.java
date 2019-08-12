package com.tian.combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombineDriver {
	public static void main(String[] args) throws Exception, IOException, InterruptedException {

		args = new String[] { "e:/git/hadoop/MapReduce/input/combine.txt",
				"e:/git/hadoop/MapReduce/output" };

		// 1. 获取Job对象 ==> 一个MapReduce程序实际上就是一个Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2. 关联jar
		job.setJarByClass(CombineDriver.class);
		// 3. 关联当前Job对应的 Mapper 和 Reducer
		job.setMapperClass(CombineMapper.class);
		job.setReducerClass(CombineReducer.class);

		// 4. 设置Mapper输出的key 和 value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5. 设置最终输出的key 和 value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6. 设置文件的输入 和 结果的输出位置
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/** 设置使用CombineTextInputFormat 及 最大的虚拟存储切片大小 */
		// job.setInputFormatClass(CombineTextInputFormat.class);
		// CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

		// job.setNumReduceTasks(2);

		// job.setCombinerClass(CombineReducer.class);
		job.setCombinerClass(MyCombiner.class);

		// 7. 提交Job
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}

}
