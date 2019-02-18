package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class STDApplyWindowFunction implements WindowFunction<SensorData, SensorData, String, TimeWindow> {
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) throws Exception {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (SensorData sensorData : input) {
            descriptiveStatistics .addValue(sensorData.getValue());
        }
        double std = descriptiveStatistics.getStandardDeviation();
        double lowerThreshold = descriptiveStatistics.getMean() - 2 * std;
        double upperThreshold = descriptiveStatistics.getMean() + 2 * std;

        for (SensorData sensorData : input) {
            if (sensorData.getValue() > lowerThreshold && sensorData.getValue() < upperThreshold) {
                out.collect(sensorData);
            }
        }
    }
}
