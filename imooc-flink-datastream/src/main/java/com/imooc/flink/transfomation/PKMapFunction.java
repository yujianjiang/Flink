package com.imooc.flink.transfomation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class PKMapFunction extends RichMapFunction<String, Access> {

    /**
     *
     * 初始化操作
     * Connection
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open" + "*****");
        super.open(parameters);
    }

    /**
     * 清理操作
     */
    @Override
    public void close() throws Exception {
        System.out.println("close" + "*****");
        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        System.out.println("*****");
        return super.getRuntimeContext();
    }

    @Override
    public Access map(String s) throws Exception {
        System.out.println("map++++++");

        String[] splits = s.split(",");
        Long time = Long.parseLong(splits[0].trim());
        String domain = splits[1].trim();
        Double traffic = Double.parseDouble(splits[2].trim());

        return new Access(time, domain, traffic);
    }
}
