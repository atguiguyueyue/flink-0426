package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 从文件中获取数据（可以设置多并行度）
//        DataStreamSource<String> streamSource = env.readTextFile("input");
        //读取Hdfs中的文件数据
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:8020/Flink课堂笔记.txt").setParallelism(2);

        streamSource.print();

        env.execute();
    }
}
