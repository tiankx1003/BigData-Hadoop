package com.tian.table1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

	private String fileName;
	private Text k;
	private TableBean v;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit(); // 获取切片对象
		FileSplit fileSplit = (FileSplit) inputSplit;

		fileName = fileSplit.getPath().getName(); //获取文件信息
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString(); //读取一行数据
		String[] fields = line.split("\t");
		
		/**
		 * 根据切片信息判断数据来自于哪个文件，以决定每条数据切割完以后生成几个字段
		 */
		if (fileName.contains("order")) {
			k.set(fields[1]);
			v.setOrderId(fields[0]);
			v.setPid(fields[1]);
			v.setAmount(Integer.parseInt(fields[2]));
			v.setFlag("order");
			v.setPname("");
		} else if(fileName.contains("pd")){
			k.set(fields[0]);
			v.setOrderId("");
			v.setOrderId(fields[2]);
			v.setPname(fields[1]);
			v.setFlag("pd");
			v.setAmount(0);
		}
		
		context.write(k, v);
		
	}

}
