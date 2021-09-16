package com.atguigu.day08;

import com.oracle.jrockit.jfr.DataType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class Flink03_TableAPI_Source_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接外部文件系统读取数据
        Schema schema = new Schema()
                .field("id", "String")
                .field("ts", "BIGINT")
//                .field("ts", DataTypes.BIGINT())
                .field("vc","Integer");

        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
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
