package com.lock.constant;

import com.lock.bean.Val;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public enum Vals implements Val {
    INT_0(0), INT_1(1), STRING_0("0"), STRING_1("1");

    private Vals(Object obj) {
        this.value = obj;
    }


    private Object value;


    public Object value() {
        return value;
    }
}
