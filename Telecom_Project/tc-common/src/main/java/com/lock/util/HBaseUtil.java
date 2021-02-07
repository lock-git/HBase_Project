package com.lock.util;

import com.lock.constant.Names;
import com.lock.constant.Vals;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * author  Lock.xia
 * Date 2021-02-07
 */
public class HBaseUtil {

    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private static ThreadLocal<Admin> adminHolder = new ThreadLocal<>();

    public static void start() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            conn = ConnectionFactory.createConnection();
            connHolder.set(conn);
        }
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = conn.getAdmin();
            adminHolder.set(admin);
        }
    }

    public static void end() throws IOException {
        Admin admin = adminHolder.get();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }
        Connection conn = connHolder.get();
        if (conn != null && !conn.isClosed()) {
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        Admin admin = adminHolder.get();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    public static void createTable(String tableName) throws IOException {
        createTable(tableName, 0);
    }

    /**
     * 创建表,自定义分区
     *
     * @param tableName
     */
    public static void createTable(String tableName, int regionCount) throws IOException {
        Admin admin = adminHolder.get();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Names.TABLE_FAMILY_INFO.value());
        tableDescriptor.addFamily(hColumnDescriptor);
        if (regionCount == 0) {
            admin.createTable(tableDescriptor);
        } else {
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }

    /**
     * 生成分区键
     *
     * @param regionCount
     * @return
     */
    public static byte[][] genSplitKeys(int regionCount) {

        int splitKeyCount = regionCount - 1;
        List<byte[]> byteList = new ArrayList<>();
        for (int i = 0; i < splitKeyCount; i++) {
            // [-∞,0|), [0|,1|), [1|,+∞)  "|"的ASCII码，在所有字符中倒数第二大
            String splitKey = i + Vals.STRING_TY.strValue();
            byte[] bytes = Bytes.toBytes(splitKey);
            byteList.add(bytes);
        }
        byteList.sort(new Bytes.ByteArrayComparator());
        byte[][] splitKeys = new byte[splitKeyCount][];
        byteList.toArray(splitKeys); // 将list转化为固定格式的数组: List<byte[]> ===> byte[][]
        return splitKeys;
    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void delTable(String tableName) throws IOException {
        Admin admin = adminHolder.get();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }


    /**
     * 创建命名空间，如果存在，不做任何处理
     */
    public static void createNamespaceNX(String namespace) throws IOException {

        Admin admin = adminHolder.get();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) { // 不存在就创建新的namespace
            createNamespace(namespace);
        }


    }


    /**
     * 创建表，如果存在，先删除后创建
     *
     * @param tablename
     */
    public static void createTableXX(String tablename, int regionCount) throws IOException {
        Admin admin = adminHolder.get();
        // 判断表是否存在
        if (admin.tableExists(TableName.valueOf(tablename))) {
            // 删除表
            delTable(tablename);
        }
        // 创建表
        createTable(tablename, regionCount);
    }

    public static Table getTable(String tableName) throws IOException {

        Connection connection = connHolder.get();
        return connection.getTable(TableName.valueOf(tableName));

    }

    /**
     * 获取分区号
     *
     * @param call
     * @param date
     * @return
     */
    public static String genRegionNum(String call, String date, int regionCount) {

        // 同一个月的放同一个分区 [20210101000000]
        String yearMonth = date.substring(0, 6);
        int callHashcode = call.hashCode();
        int yearMonthHashcode = yearMonth.hashCode();

        // 希望散列一下，无规律
        // 异或算法 [相同为0，不同为1]==》 一般用于校验,有可能为负数
        int crc = Math.abs(callHashcode ^ yearMonthHashcode);
        return String.valueOf(crc % regionCount);
    }
}
