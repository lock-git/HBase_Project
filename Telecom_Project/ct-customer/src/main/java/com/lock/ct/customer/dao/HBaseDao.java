package com.lock.ct.customer.dao;

import com.lock.constant.Names;
import com.lock.constant.Vals;
import com.lock.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * author  Lock.xia
 * Date 2021-02-07
 */
public class HBaseDao {

    private List<Put> putList = new ArrayList<>();
    private int batchSize = 100; // 批处理提高效率

    /**
     * 初始化
     */
    public void init() {


        try {
            HBaseUtil.start();
            // NX 有则不做操作
            HBaseUtil.createNamespaceNX(Names.NAMESPACE_CT.value());
            // XX 有才做操作
            HBaseUtil.createTableXX(Names.TABLE_CALLLOG.value(), Vals.INT_6.intValue());
            HBaseUtil.end();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void puts(String putStr) throws IOException {

        // 样例： call1 + '\t' +call2 + '\t' +calltime + '\t' +duration + "\n";
        String[] putArr = putStr.split("\t");
        String call1 = putArr[0];
        String call2 = putArr[1];
        String calltime = putArr[2];
        String duration = putArr[3];

        // 建表时预分区 [提前分区，将数据分开放入,防止数据倾斜]
        // 分区号 [如果分区比较多，可以补0 ==》 例如 111 个 分区，应该是 001 ，002,003,004，... ,111]
        String regionNum = HBaseUtil.genRegionNum(call1, calltime, Vals.INT_6.intValue());

        // rowKey 设计 [设计合理，避免数据倾斜]
        // rowKey ==> hash ==> index ： 取余[ex：kafka]或者位运算[ex：redis/hashMap]
        String rowKey = regionNum + "_" + call1 + "_" + call2 + "_" + calltime + "_" + duration;
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_INFO.value()),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_INFO.value()),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_INFO.value()),Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_INFO.value()),Bytes.toBytes("duration"),Bytes.toBytes(duration));
        putList.add(put);

        if (putList.size() >= batchSize) {
            HBaseUtil.start();
            Table table = HBaseUtil.getTable(Names.TABLE_CALLLOG.value());
            table.put(putList);
            table.close();
            HBaseUtil.end();
            putList.clear();
        }

    }
}
