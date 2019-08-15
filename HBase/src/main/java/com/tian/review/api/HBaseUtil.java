package com.tian.review.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/15 14:45
 */
public class HBaseUtil {
    private static Connection connection = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param families
     * @throws IOException
     */
    public static void createTable(String tableName, String... families) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table " + tableName + " was already exists!");
            admin.close();
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String family : families) {
            HColumnDescriptor familyDesc = new HColumnDescriptor(family);
            tableDesc.addFamily(familyDesc);
        }
        admin.createTable(tableDesc);
        admin.close();
    }

    /**
     * 修改表最大版本数
     *
     * @param tableName
     * @param family
     * @throws IOException
     */
    public static void modifyTable(String tableName, String family) throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table " + tableName + " dose not exists!");
            admin.close();
            return;
        }
        HColumnDescriptor familyDesc = new HColumnDescriptor(family);
        familyDesc.setMaxVersions(3);
        admin.modifyColumn(TableName.valueOf(tableName), familyDesc);
        admin.close();
    }

    /**
     * 打印所有表
     *
     * @throws IOException
     */
    public static void listTables() throws IOException {
        Admin admin = connection.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        System.out.println("-----------------------------");
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }
        System.out.println("-----------------------------");
    }

    public static void deleteTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table " + tableName + " does not exists!");
            admin.close();
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
    }

    /**
     * 查看一行数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putCell(String tableName, String rowKey, String family,
                               String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 查看一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            byte[] columnBytes = CellUtil.cloneQualifier(cell);
            byte[] valueBytes = CellUtil.cloneValue(cell);
            System.out.println(Bytes.toString(columnBytes) + "===" + Bytes.toString(valueBytes));
        }
        table.close();
    }

    /**
     * 案范围回去若干行数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void getRowSByRowRange(String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] rowBytes = CellUtil.cloneRow(cell);
                byte[] columnBytes = CellUtil.cloneQualifier(cell);
                byte[] valueBytes = CellUtil.cloneValue(cell);
                System.out.println(Bytes.toString(rowBytes) + "===" +
                        Bytes.toString(columnBytes) + "===" +
                        Bytes.toString(valueBytes));
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 通过过滤器查询指定行数据
     *
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void getRowByFilter(String tableName, String family,
                                      String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] cloneRow = CellUtil.cloneRow(cell);
                byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
                byte[] cloneValue = CellUtil.cloneValue(cell);
                System.out.println(Bytes.toString(cloneRow) + "===" +
                        Bytes.toString(cloneQualifier) + "===" +
                        Bytes.toString(cloneValue));
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 通过多个filter过滤后查询数据
     *
     * @param tableName
     * @param family
     * @param column
     * @param value1    过滤条件1
     * @param value2    过滤条件2
     * @throws IOException
     */
    public static void getRowByFilterList(String tableName, String family, String column, String value1, String value2) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(value1));
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(value2));
        filter1.setFilterIfMissing(true);
        filter2.setFilterIfMissing(true);
        FilterList filterList = new FilterList(filter1, filter2);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] cloneRow = CellUtil.cloneRow(cell);
                byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
                byte[] cloneValue = CellUtil.cloneValue(cell);
                System.out.println(Bytes.toString(cloneRow) + "===" + Bytes.toString(cloneQualifier) + "===" + Bytes.toString(cloneValue));
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除列
     * 最新版本
     * 指定时间戳可删除指定版本的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void delete(String tableName,String rowKey,
                              String family,String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));//删除最新版本
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),
                new Long(10000)); //删除指定时间戳对应版本的数据
        table.delete(delete);
    }

    public static void main(String[] args) throws IOException {
//        createTable("class","info");
//        modifyTable("class", "info");
//        listTables();
//        deleteTable("class");
//        putCell("class","1001","info","name","mark");
//        getRow("class", "1001");
//        getRowSByRowRange("student", "1001", "1002!");
//        getRowByFilter("student","info","name","ww");
//        getRowByFilterList("student", "main", "1001", "ls", "ww");
        deleteRow("student","1003");
    }
}
