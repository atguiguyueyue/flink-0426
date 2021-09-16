package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink08_SQL_Register_Table {
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


        //5.对未注册的表进行注册创建一个临时视图表
//        tableEnv.createTemporaryView("sensor", table);

        //直接对流进行注册，成为一个临时视图表
        tableEnv.createTemporaryView("sensor", waterSensorStream);

     //方式三，直接对表的执行环境调用executerSql()方法，返回的是一个TableResult对象，可以直接打印
     tableEnv.executeSql("select * from sensor where id='sensor_1'").print();

    }
}
