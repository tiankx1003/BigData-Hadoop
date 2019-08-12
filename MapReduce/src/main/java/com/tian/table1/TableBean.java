package com.tian.table1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TableBean implements Writable{
	
	private String orderId; //订单id
	private String pid; //产品id
	private String pname; //产品名
	private String flag; //产品标签
	private int amount;

	/**
	 * 必须有一个空参构造器
	 */
	
	public TableBean() {
	}

	/**
	 * 序列化和反序列化
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(pid);
		out.writeUTF(pname);
		out.writeUTF(flag);
		out.writeInt(amount);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderId = in.readUTF();
		pid = in.readUTF();
		pname = in.readUTF();
		flag = in.readUTF();
		amount = in.readInt();
	}
	
	@Override
	public String toString() {
		return orderId + "\t" + pname + "\t" + amount;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

}
