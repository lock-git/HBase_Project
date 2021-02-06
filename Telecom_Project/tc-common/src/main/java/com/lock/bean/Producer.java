package com.lock.bean;

import java.io.Closeable;

/**
 * author  Lock.xia
 * Date 2021-02-05
 * <p>
 * 生产者接口
 */
public interface Producer extends Closeable {

    public void setIn(DataIn in);

    public void setOut(DataOut out);

    public void produce();

}
