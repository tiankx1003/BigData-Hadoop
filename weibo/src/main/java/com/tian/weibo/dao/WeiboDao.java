package com.tian.weibo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        createTable(tableName, 1, families);
    }

    /**
     * 创建表
     * 含版本数设定
     *
     * @param tableName
     * @param version
     * @param families
     * @throws IOException
     */
    public void createTable(String tableName, int version, String... families) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.close();
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor familyDesc = new HColumnDescriptor(family);
            familyDesc.setMaxVersions(version);
            tableDesc.addFamily(familyDesc);
        }
        admin.createTable(tableDesc);
        admin.close();
    }

    /**
     * 插入数据
     * 若干行与若干个值
     *
     * @param tableName 表名
     * @param rowKeys   rowKey集合
     * @param family    列族
     * @param column    列名
     * @param values    值集合
     * @throws IOException
     */
    public void putCells(String tableName, List<String> rowKeys, String family, String column, String... values) throws IOException {
        List<Put> puts = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rowKey : rowKeys) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String value : values) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                puts.add(put);
//                table.put(put); //可直接插入
            }
        }
        table.put(puts);
        table.close();
    }

    public void putCells(String tableName, String rowKey, String family, String column, String... values) throws IOException {
        List<String> rowKeys = new ArrayList<>();
        rowKeys.add(rowKey);
        putCells(tableName, rowKeys, family, column, values);
    }

    public List<String> getRowKeysByRange(String tableName, String startRow, String stopRow) throws IOException {
        List<String> rowKeyList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);//获取多行传入Scan对象,返回值为类似于结果集
        for (Result result : scanner) {
            byte[] row = result.getRow();
            rowKeyList.add(Bytes.toString(row));
        }
        scanner.close();
        table.close();
        return rowKeyList;
    }

    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    public void deleteColumn(String tableName, String rowKey, String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        table.delete(delete);
        table.close();
    }

    public List<String> getRowKeysByPrefix(String tableName, String prefix) throws IOException {
        List<String> rowKeyList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] row = result.getRow();
            rowKeyList.add(Bytes.toString(row));
        }
        table.close();
        return rowKeyList;
    }


    /**
     * 根据rowKey前缀获取数据
     */
    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {
        List<String> cellList = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                cellList.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        scanner.close();
        table.close();
        return cellList;
    }
}
