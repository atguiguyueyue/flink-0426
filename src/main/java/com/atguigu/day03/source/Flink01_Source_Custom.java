package com.atguigu.day03.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink01_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.自定Source
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);
        streamSource.print();

        env.execute();
    }

    //通过实现ParallelSourceFunction这个接口，可以设置多并行度
    public static class MySource implements SourceFunction<WaterSensor>, ParallelSourceFunction<WaterSensor> {
        private Random random = new Random();
        private volatile Boolean isRunning = true;

        /**
         * 生成数据  通过SourceContext将数据发送至下游
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("s_"+random.nextInt(1000), System.currentTimeMillis(), random.nextInt(100)));
            }

        }

        /**
         * 取消任务
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
