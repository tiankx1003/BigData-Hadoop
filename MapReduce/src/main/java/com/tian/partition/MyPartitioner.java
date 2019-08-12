package com.tian.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String keyStr = key.toString();
		String phone_pre_three = keyStr.substring(0, 3);

		int partition = 4;

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
