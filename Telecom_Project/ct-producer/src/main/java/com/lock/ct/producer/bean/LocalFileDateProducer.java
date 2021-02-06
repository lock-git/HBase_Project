package com.lock.ct.producer.bean;

import com.lock.bean.DataIn;
import com.lock.bean.DataOut;
import com.lock.bean.Producer;
import com.lock.constant.Formats;
import com.lock.util.DateUtil;
import com.lock.util.NumberUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * author  Lock.xia
 * Date 2021-02-05
 * <p>
 * 本地数据生产者
 */
public class LocalFileDateProducer implements Producer {

    private DataIn in;
    private DataOut out;


    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    // 生产数据
    public void produce() {

        try {
            // 获取通讯录信息
            List<Contact> contactList = in.read(Contact.class);
            // 关闭资源并清空
            in.close();
            in = null;


            while (true) {

                // 随机从通讯录中获取两个手机号码

                int callIndexOne = new Random().nextInt(contactList.size());
                int callIndexTwo;
                while (true) {
                    callIndexTwo = new Random().nextInt(contactList.size());
                    if (callIndexOne == callIndexTwo) {
                        break;
                    }
                }

                // CAS  算法 【不断比对，直至获取不同】
                Contact callOne = contactList.get(callIndexOne);
                Contact callTwo = contactList.get(callIndexTwo);


                // 随机生成通话时间
                String startT = "20210101000000";
                String endT = "20220101000000";

                long startTime = DateUtil.parse(startT, Formats.DATE_TIMESTAMP).getTime();
                long endTime = DateUtil.parse(endT, Formats.DATE_TIMESTAMP).getTime();

                long callTime = startTime + (long) ((endTime - startTime) * Math.random());
                String callTimeString = DateUtil.format(new Date(callTime), Formats.DATE_TIMESTAMP);


                // 随机生成通话时长 ===> 格式化，不够补0
                int callDuration = new Random().nextInt(9999);
                String callDurationStr = NumberUtil.foramt(callDuration, 4);

                // 形成通话记录
                CallLog callLog = new CallLog(callOne.getTel(), callTwo.getTel(), callTimeString, callDurationStr);

                // 将通话记录输出到指定文件中
                out.wirte(callLog);


                // 匀速生产数据
                Thread.sleep(300L);
            }


        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }

        try {
            out.close();
            out = null;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
    }
}
