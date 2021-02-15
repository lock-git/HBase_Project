package com.lock.constant;

import com.lock.bean.Val;

/**
 * author  Lock.xia
 * Date 2021-02-05
 */
public enum Names implements Val {
    KAFKA_TOPIC_CALLLOG("calllog")
    , NAMESPACE_CT("ct")
    , TABLE_CALLLOG("ct:call_log")
    ,TABLE_FAMILY_INFO("info")
    ,TABLE_FAMILY_UNACTIVE("unactive");


    private Names(String name) {
        this.value = name;
    }

    private String value;


    public String value() {
        return value;
    }
}
