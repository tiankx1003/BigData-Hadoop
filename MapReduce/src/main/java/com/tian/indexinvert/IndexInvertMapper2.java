package com.tian.indexinvert;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 第二次处理map
 * 输入key为偏移量，value为整行内容
 * 输出key为分割符的前的内容即初始文件中的字符串，value为分割符后的内容，即文件名和索引
 * @author Friday
 *
 */
public class IndexInvertMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] str = line.split("--");
		k.set(str[0]);
		v.set(str[1]);
		context.write(k, v);
	}
}
