package com.tian.myinputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *  * 自定义InputFormat
 * 	1. 继承FileInputFormat类
 * 	2. 重写createRecordReader方法
 * 	3. 根据实际情况重写isSplitable方法，指定读取的文件是否可切割
 * 
 * @author JARVIS
 * @data 2019年7月23日下午8:16:27
 */
public class MyInputFormat extends FileInputFormat<Text,BytesWritable>{

	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;//我们希望读取一个完整的文件到sequenceFile中，所以不用切割，直接返回false
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		//创建RecordReader对象
		MyRecordReader recordReader = new MyRecordReader();
		
		//调用初始化方法
		recordReader.initialize(split, context);
		
		return recordReader;
	}
	
	
	
}
