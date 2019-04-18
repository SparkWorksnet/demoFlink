package net.sparkworks.functions;

import net.sparkworks.model.OutliersResult;
import net.sparkworks.model.FlaggedOutliersResult;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class STDValuesCountApplyWindowFunction implements WindowFunction<OutliersResult, FlaggedOutliersResult, String, TimeWindow> {

    // Calculate the standard deviation, set the thresholds, flag the outliers
    // Collect the FlaggedOutliersResult.
    // Applied on the outliers
    public void apply(String s, TimeWindow window, Iterable<OutliersResult> input, Collector<FlaggedOutliersResult> out) {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (OutliersResult outliersResult : input) {
            descriptiveStatistics.addValue(outliersResult.getValuesCount());
        }
        double std = descriptiveStatistics.getStandardDeviation();
        double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
        double upperThreshold = descriptiveStatistics.getMean() + 2 * std;

        input.forEach(cr -> {
            FlaggedOutliersResult flaggedOutliersResult =
                    new FlaggedOutliersResult(cr.getUrn(), cr.getTimestamp(), cr.getValuesCount(), cr.getOutliersCount());
            if (cr.getValuesCount() < lowerThreshold || cr.getValuesCount() > upperThreshold) {
                flaggedOutliersResult.setOutlier(true);
            }
            out.collect(flaggedOutliersResult);
        });
    }
}
