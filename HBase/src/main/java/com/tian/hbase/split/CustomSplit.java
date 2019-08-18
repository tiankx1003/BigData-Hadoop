package com.tian.hbase.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 创建预分区
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 16:07
 */
public class CustomSplit {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("test"));
        tableDesc.addFamily(new HColumnDescriptor("info"));
        byte[][] splitKeys = new byte[3][];
        splitKeys[0] = Bytes.toBytes("aaa");
        splitKeys[1] = Bytes.toBytes("bbb");
        splitKeys[2] = Bytes.toBytes("ccc");
        admin.createTable(tableDesc, splitKeys);
        admin.close();
    }
}
