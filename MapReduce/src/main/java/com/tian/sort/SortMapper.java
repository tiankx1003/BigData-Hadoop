package com.tian.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	FlowBean k = new FlowBean();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split("\t");
		v.set(fields[0]);
		k.setUpFlow(Long.parseLong(fields[1]));
		k.setDownFlow(Long.parseLong(fields[2]));
		k.setSumFlow(Long.parseLong(fields[3]));
		
		context.write(k, v);
	}
}
