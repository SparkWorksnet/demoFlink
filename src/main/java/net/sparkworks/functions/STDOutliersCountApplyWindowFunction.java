package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import net.sparkworks.model.FlaggedCountersResult;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class STDOutliersCountApplyWindowFunction implements WindowFunction<CountersResult, FlaggedCountersResult, String, TimeWindow> {

    // Calculate the standard deviation, set the thresholds, flag the outliers
    // Collect the FlaggedCountersResult.
    // Applied on the expected sensor values
    public void apply(String s, TimeWindow window, Iterable<CountersResult> input, Collector<FlaggedCountersResult> out) {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (CountersResult countersResult : input) {
            descriptiveStatistics.addValue(countersResult.getOutliersCount());
        }
        double std = descriptiveStatistics.getStandardDeviation();
        double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
        double upperThreshold = descriptiveStatistics.getMean() + 2 * std;

        input.forEach(cr -> {
            FlaggedCountersResult flaggedOutliersResult =
                    new FlaggedCountersResult(cr.getUrn(), cr.getTimestamp(), cr.getValuesCount(), cr.getOutliersCount());
            if (cr.getOutliersCount() < lowerThreshold || cr.getOutliersCount() > upperThreshold) {
                flaggedOutliersResult.setOutlier(true);
            }
            out.collect(flaggedOutliersResult);
        });
    }
}

