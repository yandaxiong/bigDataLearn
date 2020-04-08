package com.xiong.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


public class HbaseUtils {

    private static Configuration config = null;
    private static HBaseAdmin admin = null;

    public static Configuration getConfig() {
        config = HBaseConfiguration.create();// 配置
        config.set("hbase.zookeeper.quorum", "192.168.42.202,192.168.42.203,192.168.42.204");// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        config.set("hbase.rootdir", "hdfs://xiong0002:8020/hbase");
        config.set("fs.defaultFS", "hdfs://xiong0002:8020");
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        return config;
    }

    public static HBaseAdmin init() throws Exception {
        config = HBaseConfiguration.create();// 配置
        config.set("hbase.zookeeper.quorum", "192.168.42.202,192.168.42.203,192.168.42.204");// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        config.set("hbase.rootdir", "hdfs://xiong0002:8020/hbase");
        config.set("fs.defaultFS", "hdfs://xiong0002:8020");
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        admin = new HBaseAdmin(config);
        return admin;
    }

    /**
     * 判断表是否已存在
     */
    public static boolean isTableExist(String tableName) throws Exception {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        HbaseUtils.init();
        return admin.tableExists(tableName);
    }

    //创建表
    public static void createTable(String tableName, String... columnFamily) throws Exception {
        HbaseUtils.init();
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "已存在");
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(hTableDescriptor);

        }
    }

    //删除表
    public static void dropTable(String tableName) throws Exception {
        init();
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } else {
            System.out.println("表" + tableName + "不存在");
        }
    }

    //向表中插入数据
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws Exception {
        init();
        //创建HTable对象
        HTable hTable = new HTable(config, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
    }

    //删除多行数据
    public static void deleteMultiRow(String talbeName, String... rows) throws Exception {
        init();
        HTable hTable = new HTable(config, talbeName);
        ArrayList<Delete> deletes = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        hTable.delete(deletes);
    }

    //得到所有的数据
    public static List<Cell> getAllRows(String tableName) throws Exception {
        init();
        ArrayList<Cell> list = new ArrayList<Cell>();
        HTable hTable = new HTable(config, tableName);
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                list.add(cell);
            }
        }
        return list;
    }


    public static void main(String[] args) throws Exception {
//        boolean b = HbaseUtils.isTableExist("student");
//        HbaseUtils.createTable("fruit", "info", "other");
//        dropTable("person");
//        addRowData("fruit", "1001", "info", "name", "apple");
//        addRowData("fruit", "1001", "info", "color", "red");
//        addRowData("fruit", "1002", "info", "name", "banana");
//        addRowData("fruit", "1002", "info", "color", "yellor");
//        addRowData("fruit", "1003", "info", "name", "pear");
//        addRowData("fruit", "1003", "info", "color", "yellor");
//        addRowData("person", "1001", "basic_info", "name", "Nick");
//        addRowData("person", "1001", "basic_info", "sex", "Male");
//        addRowData("person", "1001", "basic_info", "age", "18");
//        addRowData("person", "1001", "job", "dept_no", "7981");


//        deleteMultiRow("person","person");


        List<Cell> lists = getAllRows("fruit");
        for (Cell cell: lists){
            //得到rowkey
            System.out.print(Bytes.toString(CellUtil.cloneRow(cell)));
            //得到列族
            System.out.print("\t"+Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.print("\t"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.print("\t"+Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println();
        }
    }


}
