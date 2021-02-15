package com.lock.coprocesser.consumer;

import com.lock.constant.Names;
import com.lock.constant.Vals;
import com.lock.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * author:Lock.xia
 * date:2021/2/14 - 22:14
 * <p>
 * 协处理器：插入被叫用户的数据
 *
 * 将ct-common-coprocesser 打成jar包，放入HBase的每个节点的lib目录下
 *
 */
public class InsertUnActiveDataCoprocesser extends BaseRegionObserver {

    /**
     * postPut : 插入数据之后，应该如何处理
     * <p>
     * 插入一条主叫用户数据之后，HBase自动插入一条被叫用户数据
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 主叫用户的 rowkey
        String rk = Bytes.toString(put.getRow());
        String[] cols = rk.split("_");

        String call1 = cols[1];
        String call2 = cols[3];
        String calltiem = cols[2];
        String duration = cols[4];
        String flag = cols[5];

        if("1".equals(flag)){ // 主要主叫数据插入才会触发协处理器

            String rowKey = HBaseUtil.genRegionNum(call2, calltiem, Vals.INT_6.intValue()) +
                    "_" + call2 + "_" + calltiem + "_" + call1 + "_" + duration + "_0";

            Put unActivePut = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_UNACTIVE.value()),Bytes.toBytes("call1"),
                    Bytes.toBytes(call2));
            put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_UNACTIVE.value()),Bytes.toBytes("calltime"),
                    Bytes.toBytes(calltiem));
            put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_UNACTIVE.value()),Bytes.toBytes("call1"),
                    Bytes.toBytes(call1));
            put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_UNACTIVE.value()),Bytes.toBytes("duration"),
                    Bytes.toBytes(duration));
            put.addColumn(Bytes.toBytes(Names.TABLE_FAMILY_UNACTIVE.value()),Bytes.toBytes("flag"),
                    Bytes.toBytes("0"));

            Table t = null;
            t.put(unActivePut);
        }


    }
}
