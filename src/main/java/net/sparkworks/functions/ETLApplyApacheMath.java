package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ETLApplyApacheMath implements WindowFunction<SensorData, SensorData, String, TimeWindow> {
    public void apply(String s, TimeWindow window, Iterable<SensorData> input, Collector<SensorData> out) throws Exception {
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        for (SensorData t : input) {
            summaryStatistics .addValue(t.getValue());
        }
        final SensorData outData = new SensorData();
        outData.setValue(summaryStatistics.getMean());
        outData.setUrn(input.iterator().next().getUrn());
        outData.setTimestamp(input.iterator().next().getTimestamp());
        out.collect(outData);
    }
}