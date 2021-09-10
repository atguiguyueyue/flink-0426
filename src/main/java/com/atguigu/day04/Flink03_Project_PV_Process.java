package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.并行设置为1，为了方便演示
        env.setParallelism(1);

        //3.读取文件中数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //4.使用Process（将数据转为JavaBean，过滤出pv的数据，转为Tuple2元组，累加计算）
        streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            private Integer count = 0;

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //a.将数据按照","切分
                String[] split = value.split(",");

                //b.转为JavaBean
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );

                //c.过滤出PV的数据
                if ("pv".equals(userBehavior.getBehavior())) {
                    //在if判断里面做count++ 计算出来的便是PV的个数
                    count++;
                    out.collect(Tuple2.of(userBehavior.getBehavior(), count));
                }
            }
        }).print();

        env.execute();

    }
}
