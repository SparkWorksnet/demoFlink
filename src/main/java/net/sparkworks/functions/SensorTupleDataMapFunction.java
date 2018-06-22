package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

public class SensorTupleDataMapFunction implements MapFunction<Tuple3<String, Long, Double>, SensorData> {

    public SensorData map(Tuple3<String,Long,Double> message) {
        final String[] items = ((String) message.getField(0)).split(",");

        SensorData value = new SensorData();
        value.setUrn((String) message.getField(0));
        value.setTimestamp((Long) message.getField(1));
        value.setValue((Double) message.getField(2));

        return value;
    }
}

