package com.tian.myinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyRecordReader extends RecordReader<Text, BytesWritable>{
	
	private FileSplit fileSplit;
	private Configuration conf;
	private Text currentKey = new Text();
	private BytesWritable currentValue = new BytesWritable();
	private boolean isFlag = true;
	
	/**
	 * 初始化
	 * @param split : 当前切片对象
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		fileSplit = (FileSplit) split;
		conf = context.getConfiguration();
	}
	
	/**
	 * 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(isFlag) {
			//获取文件路径
			String path = fileSplit.getPath().toString();
			//封装key
			currentKey.set(path);
			
			//获取文件系统对象
			FileSystem fs = FileSystem.get(conf);
			//获取输入流
			FSDataInputStream fis = fs.open(new Path(path));
			
			//读取整个文件的内容到字节数组中
			byte[] buf = new byte[(int)fileSplit.getLength()];
			IOUtils.readFully(fis, buf, 0, buf.length);
			
			//封装value
			currentValue.set(buf, 0, buf.length);
			
			isFlag = false;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		
		return 0;
	}

	@Override
	public void close() throws IOException {
	}
}
