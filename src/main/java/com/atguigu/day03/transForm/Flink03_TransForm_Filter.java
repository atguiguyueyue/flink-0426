package com.atguigu.day03.transForm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_TransForm_Filter {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        //3.TODO 利用Filter将偶数过滤掉
        SingleOutputStreamOperator<Integer> result = streamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {

                return value % 2 != 0;
            }
        });

        result.print();

        env.execute();

    }
}
