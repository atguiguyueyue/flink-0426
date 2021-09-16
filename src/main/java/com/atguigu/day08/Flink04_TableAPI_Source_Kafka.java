package com.atguigu.day08;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接Kafka读取数据
        Schema schema = new Schema()
                .field("id", "String")
                .field("ts", "BIGINT")
//                .field("ts", DataTypes.BIGINT())
                .field("vc","Integer");

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property("group.id", "bigdata")
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");


        //4.获取创建的临时表
        Table sensor = tableEnv.from("sensor");

        //5.动态查询数据
        Table resultTable = sensor
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        //6.将结果动态表转为流
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);

        //7.打印
        resultStream.print();

        env.execute();
    }
}
