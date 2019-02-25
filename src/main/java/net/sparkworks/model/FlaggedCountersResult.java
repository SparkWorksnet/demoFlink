package net.sparkworks.model;

public class FlaggedCountersResult extends CountersResult {

    private boolean isOutlier;

    public FlaggedCountersResult(String urn, long timestamp, long count, long outliersCount) {
        super(urn, timestamp, count, outliersCount);
    }

    public boolean isOutlier() {
        return isOutlier;
    }

    public void setOutlier(boolean outlier) {
        isOutlier = outlier;
    }
}
