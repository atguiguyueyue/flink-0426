package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 从端口读取数据 并行度只能为1
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999).setParallelism(2);

        streamSource.print();
        env.execute();



    }
}
