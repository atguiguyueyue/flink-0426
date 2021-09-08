package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return waterSensor;
            }
        });

        //TODO  将数据发送至Redis
        map.addSink(new RedisSink<>(new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build(), new RedisMapper<WaterSensor>() {
            /**
             * 指定用什么类型存数据
             * 第二个参数指的是Redis 大key
             * @return
             */
            @Override
            public RedisCommandDescription getCommandDescription() {
//                return new RedisCommandDescription(RedisCommand.HSET, "0426");
                return new RedisCommandDescription(RedisCommand.SET);
            }

            /**
             * 设置RedisKey
             * 指的是hash中的小key,一般情况指的是Redis大key
             * @param waterSensor
             * @return
             */
            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                return waterSensor.getId();
            }

            /**
             * 写入的数据
             *
             * @param waterSensor
             * @return
             */
            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                String jsonString = JSON.toJSONString(waterSensor);
                return jsonString;
            }
        }));


        env.execute();
    }
}
