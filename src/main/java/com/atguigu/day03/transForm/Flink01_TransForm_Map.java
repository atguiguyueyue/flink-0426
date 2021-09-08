package com.atguigu.day03.transForm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        //3.TODO 利用Map将获取的元素+1
        SingleOutputStreamOperator<Integer> result = streamSource.map(new MapFunction<Integer, Integer>() {
            //map方法来条数据调用一次
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("map....");
                return value + 1;
            }
        });

        result.print();

        env.execute();

    }
}
