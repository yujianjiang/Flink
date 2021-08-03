package com.imooc.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class SourceApp {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        test01(env);
        test02(env);

        env.execute("SourceApp");
    }

    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        env.setParallelism(5);
        System.out.println("source...." + source.getParallelism());

        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"pk".equals(s);
            }
        }).setParallelism(6);
        System.out.println("filter...." + filterStream.getParallelism());

        filterStream.print();
    }

    public static void test02(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(
                new NumberSequenceIterator(1, 10), Long.class
        );

        System.out.println("source...." + source.getParallelism());

        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value >= 5;
            }
        });

        System.out.println("filterStream...." + filterStream.getParallelism());
    }
}
