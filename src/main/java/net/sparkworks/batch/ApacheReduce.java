package net.sparkworks.batch;

import net.sparkworks.model.SensorData;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

public class ApacheReduce implements GroupReduceFunction<SensorData, SensorData> {
    public void reduce(Iterable<SensorData> in, Collector<SensorData> out) {
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        SensorData outData = new SensorData();
        for (SensorData sensorData : in) {
            outData.setUrn(sensorData.getUrn());
            outData.setTimestamp(sensorData.getTimestamp());
            summaryStatistics.addValue(sensorData.getValue());
        }
        outData.setValue(summaryStatistics.getMean());
        out.collect(outData);
    }
}
