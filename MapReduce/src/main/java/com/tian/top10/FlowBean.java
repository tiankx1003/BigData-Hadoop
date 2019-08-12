package com.tian.top10;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	
	long upFlow;
	long downFlow;
	long sumFlow;

	public FlowBean() {
	}

	public FlowBean(long upFlow, long downFlow, long sumFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	


	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = upFlow + downFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {

	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}

	/**
	 * 根据sumFlow进行排序 从大到小
	 */
	@Override
	public int compareTo(FlowBean o) {
		int result = 0;
		if (this.sumFlow > o.sumFlow)
			result = -1;
		else
			result = 1;
		return result;
	}

}
