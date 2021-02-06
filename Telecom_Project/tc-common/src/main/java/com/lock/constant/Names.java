package com.lock.constant;

import com.lock.bean.Val;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public enum Names implements Val {
    ;


    private Names(String name) {
        this.value = name;
    }

    private String value;


    public Object value() {
        return value;
    }
}
