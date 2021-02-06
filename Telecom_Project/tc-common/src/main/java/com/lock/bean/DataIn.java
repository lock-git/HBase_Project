package com.lock.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public interface DataIn extends Closeable {
    public void setPath(String Path);

    public Object read() throws IOException;

    // 泛型的约束 ==>  减少代码的额耦合性，提高复用率
    public <T extends Data> List<T> read(Class<T> clazz ) throws IOException;


}
