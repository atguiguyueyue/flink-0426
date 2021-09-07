package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从集合中获取数据 (并行度只能为1)
//        List<WaterSensor> waterSensors = Arrays.asList(
//                new WaterSensor("ws_001", 1577844001L, 45),
//                new WaterSensor("ws_002", 1577844015L, 43),
//                new WaterSensor("ws_003", 1577844020L, 42));
//        DataStreamSource<WaterSensor> streamSource = env.fromCollection(waterSensors).setParallelism(2);

        //TODO 从元素汇总获取数据（并行度只能设置为1）
        DataStreamSource<String> fromElements = env.fromElements("1", "2", "3", "4").setParallelism(2);

//        streamSource.print();
        fromElements.print();

        env.execute();

    }
}
