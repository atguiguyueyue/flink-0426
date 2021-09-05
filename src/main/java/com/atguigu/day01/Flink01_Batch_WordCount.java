package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件中的数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //3.先将数据按照空格切分  -> flatMap （word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        //4.reduceByKey -> 1.将相同key的数据聚和到一块  2.累加计算
        //先将相同key的数据聚和到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //再做累加计算
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        result.print();

    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        /**
         *
         * @param value 输入的数据
         * @param out 采集器可以将数据发送至下游
         * @throws Exception
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //1.先将数据按照空格切分
            String[] words = value.split(" ");
            //2.遍历数组获取到每一个单词
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
