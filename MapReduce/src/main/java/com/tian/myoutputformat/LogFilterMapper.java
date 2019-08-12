package com.tian.myoutputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 键值对的确定
 * 
 * @author JARVIS
 * @date 2019年7月26日下午6:43:09
 */
public class LogFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Text k = new Text();
	
	/**
	 * 读取到的key是每一行的偏移量，value是该行的内容
	 * 写出的key是一行的内容加换行符，value为空
	 * 
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); //读取一行数据
		line = line + "\r\n"; //最终写到文件的内容需要换行
		k.set(line);
		context.write(k, NullWritable.get());
	}

}
