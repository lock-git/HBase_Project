package com.lock.ct.customer;

import com.lock.constant.Names;
import com.lock.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * author:Lock.xia
 * date:2021/2/13 - 22:52
 *
 * 通过rowKey来获取数据【二】，startRowKey ---》 endRowKey
 *
 * 主叫已经有了，还要考虑被叫 ===》 协处理器 coprocessor
 *
 */
public class TestHBase3 {
    public static void main(String[] args) {

        try {
            HBaseUtil.start();
            Table table = HBaseUtil.getTable(Names.TABLE_CALLLOG.value());

            String call = "19683527156";
            String start = "20210101";
            String end = "20210401";

            // 分区号是通过月份计算的，反过来通过月份得到分区号
            List<String[]> startStopRows = HBaseUtil.getStartStopRows(call, start, end);
            for (String[] startStopRow : startStopRows) {
                // 根据查询条件查询数据,按照月份分开查询
                Scan scan = new Scan();
                // 包含开始
                // rowKey ==> regionNum + call1 + calldution + call2 + duration
                String startRow = startStopRow[0];
                String endRow = startStopRow[1];

                scan.setStartRow(Bytes.toBytes(startRow));

                // 不包含结束
                scan.setStartRow(Bytes.toBytes(endRow));

                // 希望可以包含结束值
//                InclusiveStopFilter stopFilter = new InclusiveStopFilter(Bytes.toBytes("5_19683537136"));
//                scan.setFilter(stopFilter);

                ResultScanner resultScanner = table.getScanner(scan);

                // 遍历得到数据
                for (Result result : resultScanner) {
                    for (Cell cell : result.rawCells()) {
                        System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    }
                }
            }

            HBaseUtil.end();


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
