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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink04_Project_UV {
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
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserIdStream = filter.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });

//        对数据进行Key操作
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserIdStream.keyBy(0);

//        累加计算
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>>() {
            //创建set集合
            HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

                //1.将userId放入set集合做去重
                uids.add(value.f1);

                //2.返回数据
                out.collect(Tuple2.of(value.f0, (long) uids.size()));
            }
        }).print();
    
        env.execute();
    }
}
