package com.imooc.flink.transfomation;


public class Access {
    private Long time;
    private String domain;
    private Double traffic;


    public Access() {
    }

    public Access(Long time, String domain, Double traffic) {
        this.time = time;
        this.domain = domain;
        this.traffic = traffic;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setTraffic(Double traffic) {
        this.traffic = traffic;
    }

    public Long getTime() {
        return time;
    }

    public String getDomain() {
        return domain;
    }

    public Double getTraffic() {
        return traffic;
    }

    @Override
    public String toString() {
        return "Access{" +
                "time=" + time +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }
}

