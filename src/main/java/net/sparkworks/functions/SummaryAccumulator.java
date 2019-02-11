package net.sparkworks.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by akribopo on 08/09/2018.
 */
public class SummaryAccumulator {
    private long count = 0L;
    private double sum = 0L;
    private double min = Long.MAX_VALUE;
    private double max = Long.MIN_VALUE;
    private double std = 0L;
    private List<Double> doubleList = new ArrayList<>();

    public void addValue(double value) {
//        sum += value;
//        count++;
//        min = Math.min(min, value);
//        max = Math.max(max, value);

        double lowerThreshold = sum / count - 2 * std;
        double upperThreshold = sum / count + 2 * std;
        if ((value > lowerThreshold && value < upperThreshold) || std == 0) {
            doubleList.add(value);
            sum += value;
            count++;
            min = Math.min(min, value);
            max = Math.max(max, value);
            double sqrMeanDiff = 0L;
            double mean = sum / count;
            for (Double val : doubleList) {
                sqrMeanDiff += Math.pow((val - mean), 2);
            }
            std = Math.sqrt(sqrMeanDiff / count);
        }
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
