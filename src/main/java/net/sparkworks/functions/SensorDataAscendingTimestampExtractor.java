package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by akribopo on 10/09/2018.
 */
public class SensorDataAscendingTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<SensorData> {
    public SensorDataAscendingTimestampExtractor() {
        super(Time.minutes(5));
    }

    @Override
    public long extractTimestamp(SensorData sensorData) {
        return sensorData.getTimestamp();
    }
}
