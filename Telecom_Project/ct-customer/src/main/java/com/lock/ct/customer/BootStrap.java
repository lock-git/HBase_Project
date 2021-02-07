package com.lock.ct.customer;

import com.lock.ct.customer.bean.CallLogConsumer;

/**
 * author  Lock.xia
 * Date 2021-02-07
 */
public class BootStrap {


    public static void main(String[] args) {


        // 创建消费者
        CallLogConsumer callLogConsumer = new CallLogConsumer();


        //消费数据
        callLogConsumer.consume();


    }


}
