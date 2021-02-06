package com.lock.util;

import com.lock.constant.Formats;

import javax.swing.tree.VariableHeightLayoutCache;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public class DateUtil {


    /**
     * 将指定的字符串转化为日期
     *
     * @param d
     * @param f
     * @return
     */
    public static Date parse(String d, Formats f) throws ParseException {

        SimpleDateFormat format = new SimpleDateFormat(f.value());
        return format.parse(d);

    }


    /**
     * 将Date类型转化为固定的格式字符串
     * @param d
     * @param f
     * @return
     */
    public static String format(Date d, Formats f) {
        SimpleDateFormat format = new SimpleDateFormat(f.value());
        return format.format(d);

    }

}
