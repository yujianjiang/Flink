package com.imooc.flink.source;

import com.imooc.flink.transfomation.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessSource implements SourceFunction<Access> {
    boolean running = true;

    @Override
    public void run(SourceContext<Access> sourceContext) throws Exception {
        String[] domains = {"imooc.com", "a.com", "b.com"};
        Random random = new Random();

        while (running) {

            for (int i = 0; i < 10; i++) {

                Access access = new Access();

                access.setTime(123L);
                access.setDomain(domains[random.nextInt(domains.length)]);
                access.setTraffic(random.nextDouble());

                sourceContext.collect(access);
            }

            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
