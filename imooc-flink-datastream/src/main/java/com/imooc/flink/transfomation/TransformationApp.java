package com.imooc.flink.transfomation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationApp {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.execute("TransformationApp");
    }
}
