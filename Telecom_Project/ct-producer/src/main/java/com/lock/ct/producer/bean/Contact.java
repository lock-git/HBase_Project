package com.lock.ct.producer.bean;

import com.lock.bean.Data;

/**
 * author  Lock.xia
 * Date 2021-02-06
 */
public class Contact extends Data {

    private String tel;

    private String username;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getTel() {

        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    @Override
    public void setContent(String content) {

        if (content != null && !"".equals(content)) {
            String[] contents = content.split("\t");
            if (contents.length == 2) {
                tel = contents[0];
                username = contents[1];
            }
        }
    }

    @Override
    public String toString() {
        return "Contact{" +
                "tel='" + tel + '\'' +
                ", username='" + username + '\'' +
                '}';
    }
}
