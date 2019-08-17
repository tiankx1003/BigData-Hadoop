package com.tian.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/16 10:23
 */
public class WeiboDao {
    public static Connection connection = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hadoop.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespace
     * @throws IOException
     */
    public void createNameSpace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {//抛异常则不存在，然后再创建
            NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDesc);
        } finally {
            admin.close();
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param families
     * @throws IOException
     */
    public void createTable(String tableName, String... families) throws IOException {
        createTable(tableName, 1, families);
    }

    public void createTable(String tableName, int versions, String... families) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table " + tableName + " already exists!");
            admin.close();
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor familyDesc = new HColumnDescriptor(family);
            familyDesc.setMaxVersions(versions);
            tableDesc.addFamily(familyDesc);
        }
        admin.createTable(tableDesc);
        admin.close();
    }

    public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public List<String> getRowKeysByPrefix(String tableName, String prefix) throws IOException {
        List<String> rowKeys = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            rowKeys.add(Bytes.toString(row));
        }
        scanner.close();
        table.close();
        return rowKeys;
    }

    public void putCells(String tableName, List<String> rowKeys, String family, String column, String value) throws IOException {
        if (rowKeys.size() == 0) return;
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        for (String rowKey : rowKeys) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        }
        table.put(puts);
        table.close();
    }

    /**
     * 通过范围获取rowKey
     * @param tableName
     * @param startRow
     * @param stopRow
     * @return
     * @throws IOException
     */
    public List<String> getRowKeyesByRange(String tableName,String startRow, String stopRow) throws IOException {
        List<String> rowKeys = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            rowKeys.add(Bytes.toString(row));
        }
        scanner.close();
        table.close();
        return null;
    }
}
