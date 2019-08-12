package com.tian.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 泛型确定
 * 
 * @author Friday
 *
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	FlowBean v = new FlowBean();
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		
		// 1 获取一行
		String line = value.toString();
		
		// 2 切割字段
		String[] fields = line.split("\t");
		
		// 3 封装对象
		// 取出手机号码
		String phoneNum = fields[1];

		// 取出上行流量和下行流量
		long upFlow = Long.parseLong(fields[fields.length - 3]);
		long downFlow = Long.parseLong(fields[fields.length - 2]);

		k.set(phoneNum);
		v.set(downFlow, upFlow);//累加流量
		
		// 4 写出
		context.write(k, v);
	}
}

