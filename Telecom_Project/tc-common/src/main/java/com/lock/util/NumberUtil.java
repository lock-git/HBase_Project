package com.lock.util;

import sun.applet.Main;

import java.text.DecimalFormat;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public class NumberUtil {


    /**
     * 将指定的数字按照指定的长度转化为字符串，不够用0补充
     *
     * @param num
     * @param len
     * @return
     */
    public static String foramt(int num, int len) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append("0");
        }
        DecimalFormat df = new DecimalFormat(sb.toString());
        return df.format(num);

    }

    public static void main(String[] args) {
        System.out.println(foramt(4, 5));

    }

}
