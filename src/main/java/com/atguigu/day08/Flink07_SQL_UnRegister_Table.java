package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Flink07_SQL_UnRegister_Table {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //3.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表(未注册的表)
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //5.写sql查询数据
//        Table resultTable = tableEnv.sqlQuery("select * from " + table + " where id='sensor_1'");

        //方式二，对Table对象调用execute()方法，返回一个TableResult对象，然后对其打印
     /*   TableResult tableResult = resultTable.execute();
        tableResult.print();
*/

     //方式三，直接对表的执行环境调用executerSql()方法，返回的是一个TableResult对象，可以直接打印
     tableEnv.executeSql("select * from " + table + " where id='sensor_1'").print();
        //6.将表转为流
//        DataStream<Row> result = tableEnv.toAppendStream(resultTable, Row.class);
//
//        result.print();
//
//        env.execute();
    }
}
