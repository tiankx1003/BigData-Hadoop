package com.tian.myoutputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LogFilterRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream tianOut;
	FSDataOutputStream otherOut;
	TaskAttemptContext context;

	public LogFilterRecordWriter(TaskAttemptContext context) {
		this.context = context;
		try {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			//确定写入的文件路径
			tianOut = fs.create(new Path("path1"));  
			otherOut = fs.create(new Path("path2"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 核心业务逻辑，将包含"tian"的日志数据写到tian.log，其他的写出到other.log
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 * @throws InterruptedException
	 */

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {

		String line = key.toString(); // key为读取到的一整行内容

		/*
		 * 判断该行是否包含"tian"
		 */
		if (line.contains("tian"))
			tianOut.write(line.getBytes()); // 写入到tian.log
		else
			otherOut.write(line.getBytes()); // 写入到other.log
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		IOUtils.closeStream(tianOut);
		IOUtils.closeStream(otherOut);
	}

}
