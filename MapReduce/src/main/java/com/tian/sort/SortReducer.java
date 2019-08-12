package com.tian.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
	
	@Override
	protected void reduce(FlowBean key, Iterable<Text> value, 
			Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		for (Text text : value) {
			context.write(text, key);
		}
		
	}

}
