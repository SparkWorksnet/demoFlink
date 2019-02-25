package net.sparkworks.model;

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

        String[] tokens = line.split("(,|;)\\s*");
        if (tokens.length != 4) {
            throw new IllegalStateException("Invalid record: " + line);
        }
        CountersResult event = new CountersResult();

        try {
            event.setUrn(tokens[0]);
            event.setTimestamp(Long.parseLong(tokens[1]));
            event.setCount(Long.parseLong(tokens[2]));
            event.setOutliersCount(Long.parseLong(tokens[3]));

        } catch (Exception e) {
            throw new IllegalStateException("Invalid field: " + line, e);
        }

        return event;
    }
}
