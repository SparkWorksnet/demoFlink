package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class STDOutliersRemovalWindowFunction implements WindowFunction<SensorData, SensorData, String, TimeWindow> {
    
    // Calculate the standard deviation, set the thresholds, remove the outliers
    // Collect the SensorData.
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) {
        
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (SensorData sensorData : input) {
            descriptiveStatistics .addValue(sensorData.getValue());
        }
        double std = descriptiveStatistics.getStandardDeviation();
        double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
        double upperThreshold = descriptiveStatistics.getMean() + 2 * std;
        
        input.forEach(sd -> {
            if (sd.getValue() < lowerThreshold || sd.getValue() > upperThreshold) {
                System.out.println(String.format("Detected and removed Outlier urn: %s, timestamp: %s, value: %s.",
                        sd.getUrn(), String.valueOf(sd.getTimestamp()), String.valueOf(sd.getValue())));
            } else {
                out.collect(sd);
            }
        });
    }
}
