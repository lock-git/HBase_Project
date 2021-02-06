package com.lock.ct.producer;

import com.lock.bean.Producer;
import com.lock.ct.producer.bean.LocalFileDateProducer;
import com.lock.ct.producer.io.LocalFileDataIn;
import com.lock.ct.producer.io.LocalFileDataOut;

/**
 * author  Lock.xia
 * Date 2021-02-05
 * <p>
 * 启动生产者
 */
public class BootStrap {
    public static void main(String[] args) {
        // 构建生产者对象
        Producer producer = new LocalFileDateProducer();

        String inPath = "F:\\WeAreFamily_LetGoGoGo\\Habase项目\\Project-CT_haibo\\contact.log";
        String outPath = "F:\\WeAreFamily_LetGoGoGo\\Habase项目\\Project-CT_haibo\\call.log";

        producer.setIn(new LocalFileDataIn(inPath));
        producer.setOut(new LocalFileDataOut(outPath));

        // 生产数据
        producer.produce();

    }
}
