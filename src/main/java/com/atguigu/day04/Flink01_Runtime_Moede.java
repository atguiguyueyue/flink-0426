package com.atguigu.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_Runtime_Moede {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 设置运行模式
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


//        //2.读取无界的数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //读取有界流数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.先利用flatMap将数据按照空格切分
        SingleOutputStreamOperator<String> wordStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
                //与前后都断开
//                .disableChaining();

        //4.将上游flatmap发送过来的每一个单词组成Tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //5.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                //value.f0 类似于 scala中的 value._0
                return value.f0;
            }
        });

        //6.累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        //开启任务
        env.execute();
    }
}
