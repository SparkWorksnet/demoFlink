package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Simple reduce function for computing average values.
 *
 * @author ichatz@gmail.com
 */
public class SensorDataAverageReduce
        implements ReduceFunction<SensorData> {

    public SensorData reduce(SensorData a, SensorData b) {
        SensorData value = new SensorData();
        value.setUrn(a.getUrn());
        value.setValue((a.getValue() + b.getValue()) / 2);
        return value;
    }

}
