package com.tian.indexinvert;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 输出的key为字段，value为文件名以及索引汇总
 * @author Friday
 *
 */
public class IndexInvertReducer2 extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Text v = new Text();
		//merge
		StringBuilder indexStr = new StringBuilder();
		for (Text value : values) {
			String fileName = value.toString();
			indexStr.append(fileName.replaceAll("\t", "-->") + " ");
			v.set(indexStr.toString());
			context.write(key, v);
		}
	}
}
