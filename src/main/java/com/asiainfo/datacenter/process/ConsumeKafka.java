package com.asiainfo.datacenter.process;

import com.alibaba.fastjson.JSONObject;
import com.asiainfo.datacenter.attr.OracleAttr;
import com.asiainfo.datacenter.attr.SaslConfig;
import com.asiainfo.datacenter.parse.CbOggMessage;
import com.asiainfo.datacenter.parse.OracleParser;
import com.asiainfo.datacenter.main.OracleEntry;
import com.asiainfo.datacenter.utils.PropertiesUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 董建斌 on 2018/9/26.
 */
public class ConsumeKafka {
    private static Logger log = Logger.getLogger(ConsumeKafka.class.getName());

    private final BlockingQueue<JSONObject> queue;
    private Properties props = null;
    private static boolean complete = false;


    public ConsumeKafka(BlockingQueue<JSONObject> queue) {
        this.queue = queue;
    }

    public void stop() {
        log.info("--------Kafka Consumer stop---------");
        this.complete = true;
        log.info("-----------Kafka Consumer stopped--------------------");
    }

    public void consume(String bootstrap, String topic, String groupid, final String client) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("max.poll.interval.ms", "300000");
        props.put("max.poll.records", "100000");
        props.put("auto.offset.reset", "latest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        String appKey = PropertiesUtil.getInstance().getProperty("sasl.appkey");
        String secretKey = PropertiesUtil.getInstance().getProperty("sasl.secretkey");
        Configuration.setConfiguration(new SaslConfig(appKey,secretKey));
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        AtomicLong atomicLong = new AtomicLong();
        while (true) {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(50000);
                records.forEach(record -> {
//                    System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s%n",
//                            client, record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    byte[] kafkaMsg = new byte[0];
                    kafkaMsg = record.value();

                    parseMsg(kafkaMsg);
                });
                consumer.commitAsync();
            }catch (Exception e)
            {
                System.out.println("kafka_Exception---------->>" + e);
            }
        }
    }

    private void parseMsg(byte[] kafkaMsg) {
        try {
            CbOggMessage oggMsg = CbOggMessage.parse(kafkaMsg);
            OracleEntry.incrReceivedFromKafkaOptCount(1);

            while (this.queue.remainingCapacity() <= 200) {
                try {
                    Thread.sleep(100l);
                } catch (InterruptedException e) {
                    log.warn("BQ does not have enough room to save operations!");
                }
            }

            String optType =  new String(oggMsg.getOperate().name()).toUpperCase();
            String optTable = new String(oggMsg.getTableName());
            String optOwner = new String(oggMsg.getSchemeName());
            if (OracleAttr.CHANGE_OWNER != null) {
                optOwner = OracleAttr.CHANGE_OWNER.get(optOwner);
            }
            StringBuilder optTableBuilder = new StringBuilder();
            optTableBuilder = new StringBuilder(optOwner + ".");

            String optSql = "";

            if (OracleParser.checkTable(oggMsg)) {
                try {
//                    StringBuilder optTableBuilder = new StringBuilder(optOwner);
                    optTableBuilder.append(optTable);
                    List<List> filedList = OracleParser.getFiledList();
                    List<List> primarykeyList = OracleParser.getPrimaryKeyList();

                    switch (oggMsg.getOperate()) {
                        case Update:
                        case Key:
                            optSql = OracleParser.jsonToUpdateOrUpdatePkSql(filedList, primarykeyList, optTableBuilder.toString());
                            break;
                        case Insert:
                            optSql = OracleParser.jsonToInsertSql(filedList, optTableBuilder.toString());
                            break;
                        case Delete:
                            optSql = OracleParser.jsonToDeleteSql(primarykeyList, optTableBuilder.toString());
                            break;
                        default:
                            log.error("Unaccepted operation:\n" + new String(kafkaMsg));
                            break;
                    }

                    JSONObject optSqlJson = new JSONObject();
                    optSqlJson.put("opt", optType);
                    optSqlJson.put("table", optTable);
                    optSqlJson.put("sql", optSql);
                    this.queue.add(optSqlJson);
//                    System.out.println("kafkaMsg: " + new String(kafkaMsg));
//                    System.out.println("optSql: " + optSql);
                } catch (Exception e) {
                    log.error("Parse to SQL ERROR : getMessage - " + new String(kafkaMsg) + "\n" + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
//            System.out.println("====================ExceptionMessage:"+e.getMessage());
//            System.out.println("====================ExceptionData: " + new String(kafkaMsg));
        }
    }

    public static void main(String[] args) {
        BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();
        ConsumeKafka kafkaConsumer = new ConsumeKafka(queue);
//        kafkaConsumer.consume("master", "S3SA048:2181,S3SA049:2181,S3SA050:2181", "test-gomewallet-group-1", "finance_gome_wallet", 1);
    }


}
