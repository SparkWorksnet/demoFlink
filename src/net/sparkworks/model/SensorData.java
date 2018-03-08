package net.sparkworks.model;

/**
 * POJO object for a SensorData single value.
 *
 * @author ichatz@gmail.com
 */
public class SensorData {

    private String urn;

    private double value;

    private long timestamp;

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
                '}';
    }
}
