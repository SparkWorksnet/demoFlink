package net.sparkworks.model;

import java.util.Date;

/**
 * POJO object for a SensorData single value.
 *
 * @author ichatz@gmail.com
 */
public class SensorData {

    private String urn;

    private double value;

    private long timestamp;

    public SensorData() {
    }

    public SensorData(String urn, double value, long timestamp) {
        this.urn = urn;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getUrn() {
        return urn;
    }

    public void setUrn(String urn) {
        this.urn = urn;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "urn='" + urn + '\'' +
                ", value=" + value +
                ", time=" + new Date(timestamp) +
                '}';
    }
    
    public static SensorData fromString(String line) {
        
        String[] tokens = line.split("(,|;)\\s*");
        if (tokens.length != 3) {
            throw new IllegalStateException("Invalid record: " + line);
        }
        SensorData event = new SensorData();
        
        try {
            event.setUrn(tokens[0]);
            event.setTimestamp(Long.parseLong(tokens[2]));
            event.setValue(Double.parseDouble(tokens[1]));
            
        } catch (Exception e) {
            throw new IllegalStateException("Invalid field: " + line, e);
        }
        
        return event;
    }
    
}
