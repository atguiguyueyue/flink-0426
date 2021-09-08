package com.atguigu.day03.transForm;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink06_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取获取数据
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);


        //TODO 3.将两个流连接到一块（同床异梦||貌合神离 各干各的事）
        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value + 1 + "");
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value + 1);
            }
        });

        process.print();
        env.execute();

    }
}
