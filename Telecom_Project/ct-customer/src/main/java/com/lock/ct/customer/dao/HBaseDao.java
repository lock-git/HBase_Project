package com.lock.ct.customer.dao;

import com.lock.constant.Names;
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
            HBaseUtil.createTableXX(Names.TABLE_CALLLOG.value());
            HBaseUtil.end();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void puts(String putStr) throws IOException {

        // call1 + '\t' +call2 + '\t' +calltime + '\t' +duration + "\n";

        String[] putArr = putStr.split("\t");
        String call1 = putArr[0];
        String call2 = putArr[1];
        String calltime = putArr[2];
        String duration = putArr[3];

        String rowKey = "";
        Put put = new Put(Bytes.toBytes(rowKey));
        putList.add(put);

        if(putList.size() >= batchSize){
            HBaseUtil.start();
            Table table = HBaseUtil.getTable(Names.TABLE_CALLLOG.value());
            table.put(putList);
            table.close();
            HBaseUtil.end();
            putList.clear();
        }


    }
}
