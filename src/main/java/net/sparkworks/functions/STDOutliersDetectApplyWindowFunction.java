package net.sparkworks.functions;

import net.sparkworks.model.FlaggedSensorData;
import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class STDOutliersDetectApplyWindowFunction implements WindowFunction<SensorData, FlaggedSensorData, String, TimeWindow> {

    // Calculate the standard deviation, set the thresholds, flag the outliers
    // Collect the FlaggedSensorData.
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<FlaggedSensorData> out) {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (SensorData sensorData : input) {
            descriptiveStatistics .addValue(sensorData.getValue());
        }
        double std = descriptiveStatistics.getStandardDeviation();
        double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
        double upperThreshold = descriptiveStatistics.getMean() + 2 * std;

        input.forEach(sd -> {
            FlaggedSensorData flaggedSensorData = new FlaggedSensorData(sd.getUrn(), sd.getValue(), sd.getTimestamp());
            if (sd.getValue() < lowerThreshold || sd.getValue() > upperThreshold) {
                flaggedSensorData.setOutlier(true);
            }
            out.collect(flaggedSensorData);
        });
    }
}
