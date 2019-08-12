package com.tian.partition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	FlowBean outV = new FlowBean();
	Text outK = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] strings = line.split("\t");
		String k = strings[1];
		outK.set(k);

		String upFlow = strings[strings.length - 3];
		String downFlow = strings[strings.length - 2];
		outV.set(Long.parseLong(upFlow), Long.parseLong(downFlow));
		context.write(outK, outV);
	}

}
