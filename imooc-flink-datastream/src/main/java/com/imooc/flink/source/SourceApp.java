package com.imooc.flink.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceApp {
    public static void main(String[] args) throws Exception{
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source...." + source.getParallelism());

        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"pk".equals(s);
            }
        });
        System.out.println("filter...." + filterStream.getParallelism());

        filterStream.print();
        env.execute("SourceApp");
    }
}
