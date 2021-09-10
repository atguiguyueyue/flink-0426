package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink07_Project_Ad {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<AdsClickLog> adsClickStream = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //4.将数据再转为tuple元组
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Integer>> provinceWithAdsToOne = adsClickStream.map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Integer>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(Tuple2.of(value.getProvince(), value.getAdId()), 1);
            }
        });
        
        //5.分组计算
        provinceWithAdsToOne
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
