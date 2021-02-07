package com.lock.ct.customer.bean;

import com.lock.bean.Consumer;
import com.lock.constant.Names;
import com.lock.ct.customer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * author  Lock.xia
 * Date 2021-02-07
 */
public class CallLogConsumer implements Consumer {

    @Override
    public void consume() {

        try {
            Properties properties = new Properties();
            properties.load(ClassLoader.getSystemResourceAsStream("kafka-consumer.properties"));


            // 构建kafka的消费者
            KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

            // 订阅主题
            kafkaConsumer.subscribe(Collections.singletonList(Names.KAFKA_TOPIC_CALLLOG.value()));

            HBaseDao dao = new HBaseDao();
            dao.init();

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("consumer data ==> " + record.value());

                    dao.puts(record.value());
                }

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
