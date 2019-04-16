package net.sparkworks.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class CountersResult {

    private String urn;

    private long timestamp;

    //in OutliersProcessor: counts the number of measurements
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of 5minute intervals (first time around)
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in 5minute intervals (second time around)
    private long valuesCount;
    
    //in OutliersProcessor: counts the number of outliers in the time interval
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the values of the 5minute intervals (first time around)
    //in ValueCountAndOutlierCountOutlierProcessor: counts the number of outliers in the outliers in 5minute intervals (second time around)
    private long valuesCountOutliersCount;

    public CountersResult() {
    }

    public CountersResult(String urn, long timestamp, long valuesCount, long valuesCountOutliersCount) {
        this.urn = urn;
        this.timestamp = timestamp;
        this.valuesCount = valuesCount;
        this.valuesCountOutliersCount = valuesCountOutliersCount;
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

    public long getValuesCountOutliersCount() {
        return valuesCountOutliersCount;
    }

    public void setValuesCountOutliersCount(long valuesCountOutliersCount) {
        this.valuesCountOutliersCount = valuesCountOutliersCount;
    }

    @Override
    public String toString() {
        return "CountersResult{" +
                "urn='" + urn + '\'' +
                ", timestamp=" + timestamp +
                ", valuesCount=" + valuesCount +
                ", valuesCountOutliersCount=" + valuesCountOutliersCount +
                '}';
    }

    public static CountersResult fromString(String line) {

        ObjectMapper mapper = new ObjectMapper();
        CountersResult countersResult = new CountersResult();
        try {
            countersResult = mapper.readValue(line, CountersResult.class);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return countersResult;
    }
}
