package com.imooc.flink.transfomation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TransformationApp {
    public static void main(String[] args) throws Exception {
        //创建上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        keyBy(env);
        env.execute("TransformationApp");
    }

    public static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        }).keyBy(x -> x.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();
    }

    /*
    public static void keyBy(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map((MapFunction<String, Access>) value -> {
            String[] splits = value.split(",");
            Long time = Long.parseLong(splits[0].trim());
            String domain = splits[1].trim();
            Double traffic = Double.parseDouble(splits[2].trim());

            return new Access(time, domain, traffic);
        });

        mapStream.keyBy(new KeySelector<Access, String>() {
            @Override
            public String getKey(Access access) throws Exception {
                return access.getDomain();
            }
        }).sum("traffic").print();

    }
    */


    /*
    public static void flatMap (StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.equals("pk");
            }
        }).print();
    }
    */



    /*
    public static void filter (StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> filterStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());

                return new Access(time, domain, traffic);
            }
        }).filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access access) throws Exception {
                return access.getTraffic() > 4000;
            }
        });
        SingleOutputStreamOperator<Access> mapStream = filterStream;

        filterStream.print();
    }
    */

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
    /*
    public static void map(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());

                return new Access(time, domain, traffic);
            }
        });

        mapStream.print();

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
    */
}
