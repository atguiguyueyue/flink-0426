package com.atguigu.day03.transForm;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //TODO 在KeyBy之前使用Process，实现flatmap相类似的功能（将单词按照空格切分并组成Tuple元组）
       /* SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });*/

        //TODO 在KeyBy之前使用Process，实现map相类似的功能（将单词按照空格切分并组成JavaBean）
        SingleOutputStreamOperator<WaterSensor> streamOperator = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(" ");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });


        //对相同key的数据进行分组并分区
        KeyedStream<WaterSensor, Tuple> keyedStream = streamOperator.keyBy("id");

        //TODO 在KeyBy之后使用Process，实现Sum相类似的功能（对单词个数进行累加）
      /*  SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //定义一个变量，用来保存累加后的结果
            private Integer count = 0;

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                count = count + 1;
                out.collect(Tuple2.of(value.f0, count));
            }
        });*/

        //TODO 在KeyBy之后使用Process，实现Sum相类似的功能（对vc值进行累加）
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //定义一个变量，用来保存累加后的vc结果
            private Integer vcSum = 0;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                vcSum = vcSum + value.getVc();
                out.collect(new WaterSensor(value.getId(), value.getTs(), vcSum));
            }
        });

        result.print();
        env.execute();

    }
}
