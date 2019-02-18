package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class IQRApplyWindowFunction implements WindowFunction<SensorData, SensorData, String, TimeWindow> {
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) throws Exception {

        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();
        for (SensorData sensorData : input) {
            descriptiveStatistics.addValue(sensorData.getValue());
        }
        double q1 = descriptiveStatistics.getPercentile(25);
        double q3 = descriptiveStatistics.getPercentile(75);

        for (SensorData sensorData : input) {
            if (sensorData.getValue() > q1 && sensorData.getValue() < q3) {
                out.collect(sensorData);
            }
        }
    }
}
