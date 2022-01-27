package stu.cfl;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建json对象用于存储数据
        JSONObject result = new JSONObject();

        // 获取数据库名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        Struct value = (Struct)sourceRecord.value();
        // 获取before数据
        JSONObject beforeJson = new JSONObject();
        Struct before = value.getStruct("before");
        if(before != null){
            Schema beforeSchema = before.schema();
            for(Field field: beforeSchema.fields()){
                Object o = before.get(field);
                beforeJson.put(field.name(), o);
            }
        }

        // 获取after数据
        JSONObject afterJson = new JSONObject();
        Struct after = value.getStruct("after");
        if(after != null){
            Schema afterSchema = after.schema();
            for(Field field: afterSchema.fields()){
                Object o = after.get(field);
                afterJson.put(field.name(), o);
            }
        }

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        // 存入结果的json结构中
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("operation", type);

        // 输出数据
        collector.collect(result.toJSONString());


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
