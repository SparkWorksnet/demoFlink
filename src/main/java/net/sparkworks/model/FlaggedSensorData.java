package net.sparkworks.model;

public class FlaggedSensorData extends SensorData {

    private boolean isOutlier;

    public FlaggedSensorData(String urn, double value, long timestamp) {
        super(urn, value, timestamp);
    }

    public boolean isOutlier() {
        return isOutlier;
    }

    public void setOutlier(boolean outlier) {
        isOutlier = outlier;
    }
}
