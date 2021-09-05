package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_Bounded_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境（流的执行环境）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2.获取有界的数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.先将数据按照空格切分  -> flatMap （word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            //1.将数据按照空格切分
            String[] words = value.split(" ");
            //2.遍历数组获取每一个单词
            for (String word : words) {
                //3.将每一个单词组成tuple元组发送到下游
                out.collect(Tuple2.of(word, 1));
//                    out.collect(new Tuple2<>(word, 1));
            }
        })
                //手动指定类型，避免因泛型擦除所导致的报错 （只在lambda表达式中会出现这种情况）
                .returns(Types.TUPLE(Types.STRING,Types.INT));

        //4.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //5.累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        //开启任务
        env.execute();
    }
}
