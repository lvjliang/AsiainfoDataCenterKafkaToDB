package com.asiainfo.datacenter.main;

import com.asiainfo.datacenter.utils.PropertiesUtil;
import com.asiainfo.datacenter.process.ShutdownHook;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by 董建斌 on 2018/9/26.
 */
public class Main {
    private static Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        log.info("---------START-----");

//        PropertyConfigurator.configure(PropertiesUtil.getInstance().getProperty("log4j.properties"));
        //检查启动参数
//        args = new String[] { "133.224.217.121:9092,133.224.217.123:9092,133.224.217.125:9092", "DBCrm2", "group2", "consumer1" };
//        args = new String[] { "10.161.11.207:7667,10.161.11.208:7667,10.161.11.209:7667", "DBCrm2_97", "97-xinxihua-01-group-01", "consumer1" };
        if (args == null || args.length != 4) {
            System.err.println("Usage:\n\tjava -jar kafka_consumer.jar ${bootstrap_server} ${topic_name} ${group_name} ${client_id}");
            log.error("Init Failer because of lacking parameters...");
            System.exit(1);
        }

        String bootstrap = args[0];
        String topic = args[1];
        String groupid = args[2];
        String client = args[3];
        OracleEntry oracleEntry = new OracleEntry();
        ShutdownHook shutdownHook = new ShutdownHook(oracleEntry);

        oracleEntry.initConfig();
        oracleEntry.startOracleEntry();
        oracleEntry.startKafkaConsumer(bootstrap, topic, groupid, client);
    }
}
