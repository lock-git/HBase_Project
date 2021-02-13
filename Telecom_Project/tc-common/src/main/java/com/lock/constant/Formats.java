package com.lock.constant;

import com.lock.bean.Val;

/**
 * author  Lock.xia
 * Date 2021-02-06
 */
public enum Formats implements Val {
    DATE_TIMESTAMP("yyyyMMddHHmmss")
    ,DATE_YM("yyyyMM")
    ;


    private String format;

    private Formats(String f) {
        format = f;
    }

    public String value() {
        return format;
    }
}
