package com.tian.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

//TODO 创建命名空间

/**
 * HBase工具类，封装静态方法
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/14 9:00
 */
public class HBaseUtil {
    private static Connection connection = null;

    /*
    静态代码块获取连接
     */
    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            //只需获取zk信息，就可以获取Region信息
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop102");
            conf.set("hbase.zookeeper.property.clientPort", "2181"); //添加端口号配置
            connection = ConnectionFactory.createConnection(conf); //赋值
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param families  列族
     * @throws IOException
     */
    public static void createTable(String tableName, String... families) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否已经存在
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table " + tableName + " was already exists!");
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        //多个列族，循环调用
        for (String family : families) {
            HColumnDescriptor familyDesc = new HColumnDescriptor(family);//列族描述
            tableDesc.addFamily(familyDesc);
        }
        admin.createTable(tableDesc);
        //放回admin
        admin.close();
    }

    /**
     * 修改表最大版本数
     *
     * @param tableName 表名
     * @param family    列族名
     * @throws IOException none
     */
    public static void modifyTable(String tableName, String family) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否不存在
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table " + tableName + " does not exists!");
            return;
        }
        HColumnDescriptor familyDesc = new HColumnDescriptor(family);
        familyDesc.setMaxVersions(3); //设置最大版本数
        admin.modifyColumn(TableName.valueOf(tableName), familyDesc);
        admin.close();
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否不存在
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table " + tableName + " does not exists!");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName)); //删除之前先disable
        admin.deleteTable(TableName.valueOf(tableName));
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
        System.out.println("---------------- Tables ----------------");
        for (TableName tableName : tableNames) {
            System.out.println(tableName.getNameAsString());
        }
        System.out.println("----------------  done  ----------------");
    }

    /**
     * 打印表的详细信息
     *
     * @param tableName
     * @throws IOException
     */
    @Deprecated
    public static void descTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否不存在
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.err.println("table " + tableName + " does not exists!");
            return;
        }
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        Map<ImmutableBytesWritable, ImmutableBytesWritable> values = tableDescriptor.getValues();
        Set<Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable>> entries = values.entrySet();
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : entries) {
            ImmutableBytesWritable value = entry.getValue();
            System.out.println(value.toString());
        }
    }

    /**
     * 添加数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putCell(String tableName, String rowKey, String family, String column, String value)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        //为put对象绑定一行数据对应的信息
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 查看一行的数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addColumn(); 获取某一列
//        get.setMaxVersions(3); 设置返回最大版本数
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            /*
            下面连个方法的返回值相同,并不能返回需要的内容
             */
            byte[] valueArray = cell.getValueArray();
            byte[] familyArray = cell.getFamilyArray();
            System.out.println(valueArray == familyArray); //验证返回值相同
            byte[] valueBytes = CellUtil.cloneValue(cell);//获取value对应的字节数组
            byte[] columnBytes = CellUtil.cloneQualifier(cell);//获取列名对应的字节数组
            System.out.println(Bytes.toString(columnBytes) + "-" + Bytes.toString(valueBytes));
        }
        table.close();
    }

    /**
     * 按范围获取指定行数据
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void getRowsByRowRange(String tableName, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        ResultScanner scanner = table.getScanner(scan); //是一个连接，需要关闭，内容为结果集
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] rowBytes = CellUtil.cloneRow(cell);
                byte[] valueBytes = CellUtil.cloneValue(cell);//获取value对应的字节数组
                byte[] columnBytes = CellUtil.cloneQualifier(cell);//获取列名对应的字节数组
                System.out.println(Bytes.toString(rowBytes) + "-"
                        + Bytes.toString(columnBytes) + "-"
                        + Bytes.toString(valueBytes));
            }
        }
        scanner.close(); //scanner需要关闭
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
    public static void getRowByColumn(String tableName, String family, String column, String value)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(value));
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(family),
                Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(value));
        filter.setFilterIfMissing(true); //如果没有该过滤条件则，直接过滤
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);//连个过滤器为与的逻辑关系
        filterList.addFilter(filter);
        filterList.addFilter(filter1);
//        scan.setFilter(filter);
        scan.setFilter(filterList); //多过滤器时传入过滤器集合
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                byte[] rowBytes = CellUtil.cloneRow(cell);
                byte[] valueBytes = CellUtil.cloneValue(cell);//获取value对应的字节数组
                byte[] columnBytes = CellUtil.cloneQualifier(cell);//获取列名对应的字节数组
                System.out.println(Bytes.toString(rowBytes) + "-"
                        + Bytes.toString(columnBytes) + "-"
                        + Bytes.toString(valueBytes));
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 删除一行
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除列(最新版本，所有版本)
     * 删除列中指定时间戳的版本
     * 删除最新版本，生成的时间戳和最新版本数据的时间戳一致
     * 删除时指定时间戳就可以删除时间戳对应的指定版本
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void delete(String tableName, String rowKey,String family,String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column)); //
        delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),
                new Long(100000)); //删除时指定时间戳就可以删除时间戳对应的指定版本
//        delete.addColumns(Bytes.toBytes(family),Bytes.toBytes(column));//删除所有版本
        table.delete(delete);
        table.close();
    }

    /**
     * main方法测试工具类方法
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
//        createTable("class", "info");
//        modifyTable("class","info");
//        dropTable("class");
//        listTables();
//        descTable("class"); //TODO 待补充
//        putCell("class", "1001", "info", "name", "0508");
//        getRow("class","1001");
//        getRowsByRowRange("student","1001","1004");
//        getRowByColumn("student", "info", "name", "zs");
        deleteRow("student", "1001");
    }
}
