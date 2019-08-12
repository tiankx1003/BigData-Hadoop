package com.tian.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 泛型类型的确定？
 * 传入为正行数据，偏移量，和字符串
 * 传出的为切分出的单词，和value恒定为1
 * @author Friday
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		// 读取一行
		String line = value.toString();
		
		// 按照空格切分字符串
		String[] words = line.split(" ");
		
		// 把切割后的词作为key，value为1写入数据
		for (String word : words) {
			
			k.set(word);
			context.write(k, v);
		}
	}

}
