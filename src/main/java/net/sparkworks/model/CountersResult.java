package net.sparkworks.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class CountersResult {

    private String urn;

    private long timestamp;

    private long count;

    private long outliersCount;

    public CountersResult() {
    }

    public CountersResult(String urn, long timestamp, long count, long outliersCount) {
        this.urn = urn;
        this.timestamp = timestamp;
        this.count = count;
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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getOutliersCount() {
        return outliersCount;
    }

    public void setOutliersCount(long outliersCount) {
        this.outliersCount = outliersCount;
    }

    @Override
    public String toString() {
        return "CountersResult{" +
                "urn='" + urn + '\'' +
                ", timestamp=" + timestamp +
                ", count=" + count +
                ", outliersCount=" + outliersCount +
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
