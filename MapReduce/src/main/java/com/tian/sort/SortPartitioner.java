package com.tian.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner extends Partitioner<FlowBean, Text> {

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {

		String keyStr = value.toString();
		String phone_pre_three = keyStr.substring(0, 3);

		int partition = 0;

		if ("136".equals(phone_pre_three))
			partition = 0;
		else if ("137".equals(phone_pre_three))
			partition = 1;
		else if ("138".equals(phone_pre_three))
			partition = 2;
		else if ("139".equals(phone_pre_three))
			partition = 3;
		
		return partition;
	}

}
