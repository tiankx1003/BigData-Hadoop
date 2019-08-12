package com.tian.myoutputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 输出的key为整行数据，输出的value为空
 * 
 * 
 * @author JARVIS
 * @date 2019年7月26日下午6:46:53
 */
public class LogFilterOutputFormat extends FileOutputFormat<Text, NullWritable> {

	/**
	 * 返回一个RecordWriter子类的实例
	 * 
	 * @param job
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		return new LogFilterRecordWriter(job);
		
	}

}
