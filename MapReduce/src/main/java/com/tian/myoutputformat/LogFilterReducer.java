package com.tian.myoutputformat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
	/**
	 * 传入的key是每行的内容，value为空
	 * 传出的key是每行的内容，value为空
	 * 
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		//直接写出
		context.write(key, NullWritable.get());
	}

}
