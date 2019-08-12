package com.tian.top10;


import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 写出内容key为手机号，value为bean对象
 * @author Friday
 *
 */

public class Top10Reducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	TreeMap<FlowBean, Text> flowMap = new TreeMap<>();
	Text k = new Text();
	/**
	 * 汇总每组并选出前十
	 */
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			k = value;
			FlowBean flowBean = new FlowBean(key.getUpFlow(),key.getDownFlow());
			flowMap.put(flowBean, k);
			
			if(flowMap.size() > 10) {
				flowMap.remove(flowMap.lastKey());
			}
		}
	}
	
	@Override
	protected void cleanup(Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		Iterator<FlowBean> iterator = flowMap.keySet().iterator();
		while(iterator.hasNext()) {
			k = flowMap.get(iterator.next());
			context.write(k, iterator.next());
		}
	}
}
