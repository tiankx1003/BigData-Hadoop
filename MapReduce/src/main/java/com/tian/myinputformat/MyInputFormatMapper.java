package com.tian.myinputformat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyInputFormatMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{
	
	/**
	 * 
	 * @param key : 文件路径(含路径名)
	 * @param value : 文件的内容
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, value);
	}

}
