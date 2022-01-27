package stu.cfl.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.sys.Prop;

import java.util.Properties;

public class KafkaUtil {

    private static String brokers = "flink101:9092,flink102:9092,flink103:9092";

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic){
        /**
         * topic: 主题名
         */
        return new FlinkKafkaProducer<String>(
                "flink101:9092,flink102:9092,flink103:9092",
                topic,
                new SimpleStringSchema()
        );
        /**
         * String brokerList,
         * String topicId,
         * SerializationSchema<IN> serializationSchema
         */
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
        /**
         * String topic,
         * KafkaDeserializationSchema<T> deserializer,
         * Properties props
         */

    }
}
