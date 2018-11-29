package net.sparkworks.functions;

/**
 * Created by akribopo on 08/09/2018.
 */
public class SummaryAccumulator {
    private long count = 0L;
    private double sum = 0L;
    private double min = Long.MAX_VALUE;
    private double max = Long.MIN_VALUE;

    public void addValue(double value) {
        sum += value;
        count++;
        min = Math.min(min, value);
        max = Math.max(max, value);
    }

    public SummaryAccumulator merge(SummaryAccumulator summaryAccumulator) {
        sum += summaryAccumulator.sum;
        count += summaryAccumulator.count;
        min = Math.min(min, summaryAccumulator.min);
        max = Math.max(max, summaryAccumulator.max);
        return this;
    }

    public long getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

}
