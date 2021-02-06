package com.lock.ct.producer.io;

import com.lock.bean.Data;
import com.lock.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public class LocalFileDataIn implements DataIn {

    private BufferedReader reader = null; // 文件一行行读取

    public LocalFileDataIn(String path) {
        setPath(path);
    }

    public void setPath(String Path) { // 字符流和字节流转化 ： InputStreamReader ==》 字符流转化为字节流
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(Path), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {

        ArrayList<T> ts = new ArrayList<T>();

        try {
            String line;
            while ((line = reader.readLine()) != null) {

                T t = clazz.newInstance(); // 反射获取对象
                t.setContent(line);
                ts.add(t);

            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return ts;
    }

    public Object read() throws IOException {
        return null;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
