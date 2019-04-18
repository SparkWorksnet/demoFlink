package net.sparkworks.model;

public class FlaggedOutliersResult extends OutliersResult {

    private boolean isOutlier;

    public FlaggedOutliersResult(String urn, long timestamp, long count, long outliersCount) {
        super(urn, timestamp, count, outliersCount);
    }

    public boolean isOutlier() {
        return isOutlier;
    }

    public void setOutlier(boolean outlier) {
        isOutlier = outlier;
    }
}
