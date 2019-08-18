package com.tian.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 创建连接，实现数据库操作
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 18:24
 */
public class WeiboDao {
    private static Connection connection = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     * 通过异常判断命名空间是否存在
     *
     * @param namespace 命名空间
     * @throws IOException
     */
    public void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) { //判断命名空间是否存在
            NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDesc);
        } finally {
            admin.close();
        }
    }

    /**
     * 创建表
     * 不含版本数设定
     *
     * @param tableName
     * @param families
     * @throws IOException
     */
    public void createTable(String tableName, String... families) throws IOException {
        createTable(tableName,1,families);
    }

    public void createTable(String tableName, int version, String... families) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        admin.createTable(tableDesc);
        admin.close();
    }
}
