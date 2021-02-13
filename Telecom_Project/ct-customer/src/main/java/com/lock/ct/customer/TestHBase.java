package com.lock.ct.customer;

import com.lock.constant.Names;
import com.lock.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * author:Lock.xia
 * date:2021/2/10 - 20:32
 *
 * 测试过滤查询数据 filter ===> 效率比较低
 *
 *
 */
public class TestHBase {
    public static void main(String[] args) {

        try {
            // 获取表
            HBaseUtil.start();
            Table table = HBaseUtil.getTable(Names.TABLE_CALLLOG.value());
            String call = "19683527156";
            String start = "20210101";
            String end = "20210201";

            // 如果根据条件查询hbase中的数据，可以采用过滤器的方式
            Scan scan = new Scan();

            // 条件一[满足主叫电话号码为 19683527156]
            Filter f1 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("call1")
            , CompareFilter.CompareOp.EQUAL,Bytes.toBytes(call));
            // 条件二[满足被叫用户号码为 19683527156]
            Filter f2 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("call2")
            , CompareFilter.CompareOp.EQUAL,Bytes.toBytes(call));

            // 以上两个条件满足一个即可
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterList.addFilter(f1);
            filterList.addFilter(f2);

            // 过滤条件中满足时间区间【 20210101 ~ 20210201】
            Filter f3 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("calltime")
                    , CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(start));

            Filter f4 = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("calltime")
                    , CompareFilter.CompareOp.LESS,Bytes.toBytes(end));
            FilterList finalFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            finalFilter.addFilter(filterList);
            finalFilter.addFilter(f3);
            finalFilter.addFilter(f4);

            scan.setFilter(finalFilter);
            ResultScanner resultScanner = table.getScanner(scan);

            // 遍历得到数据
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                }
            }
            HBaseUtil.end();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
