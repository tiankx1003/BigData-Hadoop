package com.tian.hbase.mr;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 从HBase读取数据
 * ImmutableBytesWritable实现了WritableComparable接口
 * Put没有实现上述接口，HBase为我们提供了Put的序列化器，TODO 源码
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 15:29
 */
public class ReadMapper extends TableMapper<ImmutableBytesWritable, Put> {
    /**
     * 读取特定的列
     * @param key rowKey
     * @param value Result对象
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell))))
//                put.addColumn();
                put.add(cell);
        }
        context.write(key,put);
    }
}
