package net.sparkworks.functions;

public class OutliersDetectAccumulator {

    private long count = 0L;

    private long outlierCount = 0L;

    public void addValue() {
        count++;
    }

    public void addOutlierValue() {
        outlierCount++;
    }

    public OutliersDetectAccumulator merge(OutliersDetectAccumulator outliersDetectAccumulator) {
        count += outliersDetectAccumulator.count;
        outlierCount += outliersDetectAccumulator.outlierCount;
        return this;
    }

    public long getCount() {
        return count;
    }

    public long getOutlierCount() {
        return outlierCount;
    }
}
