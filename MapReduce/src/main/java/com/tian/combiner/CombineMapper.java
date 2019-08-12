package com.tian.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable v = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] split = line.split(" ");
		
		for (String word : split) {
			k.set(word);
			context.write(k,v);
		}
	}

}
