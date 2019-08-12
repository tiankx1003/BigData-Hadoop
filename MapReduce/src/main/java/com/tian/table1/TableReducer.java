package com.tian.table1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<TableBean> values,
			Reducer<Text, TableBean, TableBean, NullWritable>.Context context)
			throws IOException, InterruptedException {

		List<TableBean> orders = new ArrayList<>();
		TableBean pd = new TableBean();

		for (TableBean tableBean : values) {

			if ("order".equals(tableBean.getFlag())) {
				try {
					TableBean order = new TableBean();
					BeanUtils.copyProperties(order, tableBean);
					orders.add(order);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				try {
					BeanUtils.copyProperties(pd, tableBean);
				} catch (Exception e) {

				}
			}

		}

		/**
		 * join 将pd对象中的pname属性值复制给从orders集合迭代出的内个TabBean对象的pname属性上
		 */
		for (TableBean tableBean : orders) {
			tableBean.setPname(pd.getPid());
			context.write(tableBean, NullWritable.get());

		}

	}

}
