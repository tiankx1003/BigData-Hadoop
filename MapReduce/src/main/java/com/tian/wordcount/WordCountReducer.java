package com.tian.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 泛型确定 
 * 传入数据为Mapper传出数据 
 * 传出数据为单词和每个单词的个数
 * 
 * @author Friday
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	int sum;// 用于存放单词个数
	IntWritable v = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// 累加求和
		sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}

		// 输出
		v.set(sum);
		context.write(key, v);
	}
}
