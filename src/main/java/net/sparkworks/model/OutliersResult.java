package net.sparkworks.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class OutliersResult {

    private String urn;

    private long timestamp;

    //in OutliersProcessor: counts the number of measurements
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of 5minute intervals (first time around)
    private long valuesCount;
    
    //in OutliersProcessor: counts the number of outliers in the time interval
    //in ValueCountAndOutlierCountOutlierProcessor (second time): counts the number of measurements
    private long outliersCount;

    // in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the values of the 5minute intervals
    // (first time around)
    private long outliersOnValuesCount;

    // in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the outliers in 5minute intervals
    // (second time around)
    private long outliersOnOutliersCount;

    public OutliersResult() {
    }

    public OutliersResult(String urn, long timestamp, long valuesCount, long outliersCount) {
        this.urn = urn;
        this.timestamp = timestamp;
        this.valuesCount = valuesCount;
        this.outliersCount = outliersCount;
    }

    public String getUrn() {
        return urn;
    }

    public void setUrn(String urn) {
        this.urn = urn;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getValuesCount() {
        return valuesCount;
    }

    public void setValuesCount(long valuesCount) {
        this.valuesCount = valuesCount;
    }

    public long getOutliersCount() {
        return outliersCount;
    }

    public void setOutliersCount(long outliersCount) {
        this.outliersCount = outliersCount;
    }

    public long getOutliersOnValuesCount() {
        return outliersOnValuesCount;
    }

    public void setOutliersOnValuesCount(long outliersOnValuesCount) {
        this.outliersOnValuesCount = outliersOnValuesCount;
    }

    public long getOutliersOnOutliersCount() {
        return outliersOnOutliersCount;
    }

    public void setOutliersOnOutliersCount(long outliersOnOutliersCount) {
        this.outliersOnOutliersCount = outliersOnOutliersCount;
    }

    @Override
    public String toString() {
        return "OutliersResult{" +
                "urn='" + urn + '\'' +
                ", timestamp=" + timestamp +
                ", valuesCount=" + valuesCount +
                ", outliersCount=" + outliersCount +
                ", outliersOnValuesCount=" + outliersOnValuesCount +
                ", outliersOnOutliersCount=" + outliersOnOutliersCount +
                '}';
    }

    public static OutliersResult fromString(String line) {

        ObjectMapper mapper = new ObjectMapper();
        OutliersResult outliersResult = new OutliersResult();
        try {
            outliersResult = mapper.readValue(line, OutliersResult.class);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return outliersResult;
    }
}
