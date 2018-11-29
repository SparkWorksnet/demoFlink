package net.sparkworks.model;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

/**
 * Created by akribopo on 09/09/2018.
 */
public class SummaryResult {

    private String urn;

    private long start;

    private double average;
    private double min;
    private double max;

    private long count;
    private double sum;


    public void setStart(long start) {
        this.start = start;
    }

    public void setUrn(String urn) {
        this.urn = urn;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "SummaryResult{" +
                "start=" + start +
                ", start=" +  new Date(start) +
                ", urn='" + urn + '\'' +
                ", average=" + average +
                ", min=" + min +
                ", max=" + max +
                ", count=" + count +
                ", sum=" + sum +
                '}';
    }
}
