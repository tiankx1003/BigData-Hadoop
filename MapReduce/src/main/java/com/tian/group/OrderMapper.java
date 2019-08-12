package com.tian.group;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
	OrderBean k = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] splits = line.split("\t");
		
		k.setOrder_id(Integer.parseInt(splits[0]));
		k.setPrice(Double.parseDouble(splits[2]));
		
		context.write(k, NullWritable.get());
	}
}
