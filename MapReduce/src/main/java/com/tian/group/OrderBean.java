package com.tian.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private int order_id;
	private double price;

	public OrderBean(int order_id, double price) {
		this.order_id = order_id;
		this.price = price;
	}

	public OrderBean() {
	}

	public int getOrder_id() {
		return order_id;
	}

	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(order_id);
		out.writeDouble(price);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		order_id = in.readInt();
		price = in.readDouble();
	}

	/**
	 * 排序规则: 按照单号由小到大，如果单号相同，按照商品价格从大到小
	 * 
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(OrderBean o) {
		int result;
		if (this.order_id > o.getOrder_id())
			result = 1;
		else if (this.order_id > o.getOrder_id())
			result = -1;
		else {
			if (this.price > o.getPrice())
				result = -1;
			else if (this.price > o.getPrice())
				result = 1;
			else
				result = 0;
		}
		return result;
	}

	@Override
	public String toString() {
		return order_id + "\t" + price;
	}

}
