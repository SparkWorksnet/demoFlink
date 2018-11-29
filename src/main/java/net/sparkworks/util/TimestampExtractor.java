package net.sparkworks.util;

import net.sparkworks.model.SensorData;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Extracts the timestamp from a sensor data and uses it for the timestamp and watermak of the event.
 *
 * @author ichatz@gmail.com
 */
public class TimestampExtractor
        implements AssignerWithPeriodicWatermarks<SensorData> {

    /**
     * The current timestamp.
     */
    private long currentTimestamp = Long.MIN_VALUE;

    public long extractTimestamp(SensorData element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    public final Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }


}
