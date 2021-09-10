package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_Project_PV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        
        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");
        
        //3.将数据转为JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
            }
        });
        
        //4.过滤出pv行为的数据
        SingleOutputStreamOperator<UserBehavior> filter = userBehaviorStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });
        
        //5.将数据组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvToOneStream = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1);
            }
        });

//        对数据进行Key操作
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pvToOneStream.keyBy(0);

//        累加计算
        keyedStream.sum(1).print();
//        SingleOutputStreamOperator<Integer> process = pvToOneStream.process(new ProcessFunction<Tuple2<String, Integer>, Integer>() {
//            private Integer count = 0;
//
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Integer> out) throws Exception {
//                count += 1;
//                out.collect(count);
//            }
//        });
//
//        process.print("pv");

        env.execute();
    }
}
