package stu.cfl.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import stu.cfl.bean.OrderWide;
import stu.cfl.bean.PaymentInfo;
import stu.cfl.bean.PaymentWide;
import stu.cfl.utils.KafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 支付宽表：支付表没有到订单明细，支付金额没有细分到商品上， 没有办法统计商品级的支付状况。
 * 所以本次宽表的核心就是要把支付表的信息与订单宽表关联上。
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        // TODO: 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        env.setStateBackend(new FsStateBackend("hdfs://flink101:8020/realTimeSystem-Flink"));
        env.enableCheckpointing(5000);
        // 设置ck模式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置ck延迟时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 设置最大并存的ck数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置两ck最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 重启策略
        //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        */

        // TODO: 加载流
        String groupId = "PaymentWideApp";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "DWM_ORDER_WIDE";
        String paymentWideSinkTopic = "DWM_PAYMENT_WIDE";
        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(data -> JSONObject.parseObject(data, OrderWide.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderWide>() {
                                            @Override
                                            public long extractTimestamp(OrderWide element, long recordTimestamp) {

                                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                                try {
                                                    return sdf.parse(element.getCreate_time()).getTime();
                                                } catch (ParseException e) {
                                                    e.printStackTrace();
                                                    return recordTimestamp;
                                                }
                                            }
                                        })
                );

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(KafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(data -> JSONObject.parseObject(data, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<PaymentInfo>() {

                                            @Override
                                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                                try {
                                                    return sdf.parse(element.getCreate_time()).getTime();
                                                } catch (ParseException e) {
                                                    e.printStackTrace();
                                                    return recordTimestamp;
                                                }
                                            }
                                        })
                );

        // TODO: 双流join，实现关联操作
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        // TODO: 写入kafka
        paymentWideDS.map(JSON::toJSONString)
                .addSink(KafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));
//        paymentWideDS.print();
        // TODO: 执行
        env.execute("PaymentWideApp");
    }
}
