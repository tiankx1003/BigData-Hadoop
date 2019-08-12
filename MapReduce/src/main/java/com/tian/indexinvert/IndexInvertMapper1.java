package com.tian.indexinvert;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexInvertMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	String name;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//get file name
		FileSplit split = (FileSplit) context.getInputSplit();
		name = split.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split(" ");
		for (String split : splits) {
			k.set(split + "--" + name); //拼接文件名
			context.write(k, v);
		}
		
	}

}
