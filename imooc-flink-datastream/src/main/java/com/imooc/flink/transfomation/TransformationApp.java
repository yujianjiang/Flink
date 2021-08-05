package com.imooc.flink.transfomation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformationApp {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        map(env);
        env.execute("TransformationApp");
    }


    /**
     * 读进来的数据是一行行的，也字符串类型
     *
     * 每一行数据 ==> Access
     *
     * 将map算子对应的函数作用到DataStream，产生一个新的DataStream
     *
     * map会作用到已有的DataStream这个数据集中的每一个元素上
     *
     */
    public static void map(StreamExecutionEnvironment env) {

//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//
//        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String value) throws Exception {
//                String[] splits = value.split(",");
//                Long time = Long.parseLong(splits[0].trim());
//                String domain = splits[1].trim();
//                Double traffic = Double.parseDouble(splits[2].trim());
//
//                return new Access(time, domain, traffic);
//            }
//        });
//
//        mapStream.print();

        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);  // map  * 2 = 2
        list.add(2);  // map  * 2 = 4
        list.add(3);  // map  * 2 = 6
        DataStreamSource<Integer> source = env.fromCollection(list);

        source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }).print();
    }
}
