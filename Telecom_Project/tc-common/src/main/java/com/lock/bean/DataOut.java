package com.lock.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public interface DataOut extends Closeable {

    public void setPath(String Path);

    public void wirte(Object obj) throws IOException;

}
