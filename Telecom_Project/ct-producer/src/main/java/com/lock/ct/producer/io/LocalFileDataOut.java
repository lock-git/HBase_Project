package com.lock.ct.producer.io;

import com.lock.bean.DataOut;

import java.io.*;

/**
 * author  Lock.xia
 * Date 2021-02-05
 * <p>
 * GBK : 2个字节
 * UTF-8 ：3个字节
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    public void setPath(String Path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(Path),"UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void wirte(Object obj) throws IOException {
        wirte(obj.toString());
    }

    public void wirte(String s) throws IOException {
       writer.print(s);
       writer.flush();

    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}



