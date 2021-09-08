package com.atguigu.day03.TransForm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_TransForm_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从元素中获取获取数据
//        DataStreamSource<String> streamSource = env.fromElements("s1_1","s2_2","s3_3");
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.TODO 利用FlatMap将获取的数据按照下滑线拆分，拆分成两条数据
        SingleOutputStreamOperator<String> result = streamSource.flatMap(new MyRichFun());

        result.print();

        env.execute();

    }
    public static class MyRichFun extends RichFlatMapFunction<String,String>{

        //声明周期方法。open方法在程序开始后最先调用,并且每个并行实例调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open....");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            String[] strings = value.split(" ");
            for (String s : strings) {
                out.collect(s);
            }
        }

        //声明周期方法。close方法在程序开始后最后调用，默认情况下每个并行实例调用一次，但是在读文件时调用两次
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}
