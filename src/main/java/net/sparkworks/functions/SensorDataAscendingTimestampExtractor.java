package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by akribopo on 10/09/2018.
 */
public class SensorDataAscendingTimestampExtractor extends AscendingTimestampExtractor<SensorData> {
    @Override
    public long extractAscendingTimestamp(SensorData element) {
        return element.getTimestamp();
    }
}
