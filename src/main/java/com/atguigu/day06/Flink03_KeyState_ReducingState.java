package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink03_KeyState_ReducingState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        //4.将相同id的数据聚和到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //5.计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 1.定义状态
            private ReducingState<Integer> reducingState;


            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducing-State", new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                }, Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //TODO 3.使用状态
                //将当前的数据存到状态中做累加计算
                reducingState.add(value.getVc());
                //取出累加后的结果
                Integer sum = reducingState.get();
                out.collect(value.getId()+"_"+sum);
            }
        }).print();

        env.execute();
    }
}
