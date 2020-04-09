package cn.fengsong97.tool;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaConsumerTool {
    static KafkaConsumer kafkaConsumer=null;

    public static KafkaConsumer get(String brokerlist,String groupid){
        if (kafkaConsumer!=null) return kafkaConsumer;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerlist);
        props.put("client.id", "getpartitions_date");
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "10000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("kerberos.auth.enable", "true");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("security.protocol", "SASL_PLAINTEXT");

        kafkaConsumer = new KafkaConsumer<Integer, String>(props);
        return  kafkaConsumer;
    }
}
