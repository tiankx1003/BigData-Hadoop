package com.tian.top10;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 读取整行
 * 写出为key为对象，value为手机号
 * kv封装成一个TreeMap调用自定义排序
 * 添加进新的内容后，根据排序规则自动去除最后一个，保持个数为10
 * @author Friday
 *
 */
public class Top10Mapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	/** 如果声明为Map类的引用则无法调用flowMap.lastKey()方法 */
	TreeMap<FlowBean,Text> flowMap = new TreeMap<>();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		Text v = new Text();
		
		String line = value.toString();
		String[] split = line.split("\t");
		
		//切割的第一段为手机号
		v.set(split[0]);
		//其余的内容为对象的属性(流量信息)
		//TODO 如果最终结果有问题，可尝试声明静态变量flowBean并个flowMap添加值
		//flowMap.put(flowBean,v);
		flowMap.put(new FlowBean(Long.parseLong(split[1]), Long.parseLong(split[2]), Long.parseLong(split[3])), v);
		
		/**
		 * 只保留前十
		 */
		if(flowMap.size() > 10) {
			flowMap.remove(flowMap.lastKey());
		}
	}
	
	/**
	 * 写出数据
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		/*
		 * 迭代
		 */
		Iterator<FlowBean> iterator = flowMap.keySet().iterator();
		while(iterator.hasNext()) {
			FlowBean bean = iterator.next();
			context.write(bean, flowMap.get(bean)); //TODO 逻辑或许可以优化
		}
		
	}
	
	

}
