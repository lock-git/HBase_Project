package com.lock.util;

import com.lock.constant.Names;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

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
        Admin admin = adminHolder.get();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Names.TABLE_FAMILY_INFO.value());
        tableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(tableDescriptor);
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
    public static void createTableXX(String tablename) throws IOException {
        Admin admin = adminHolder.get();
        // 判断表是否存在
        if (admin.tableExists(TableName.valueOf(tablename))) {
            // 删除表
            delTable(tablename);
        }
        // 创建表
        createTable(tablename);
    }

    public static Table getTable(String tableName) throws IOException {

        Connection connection = connHolder.get();
        return connection.getTable(TableName.valueOf(tableName));

    }
}
