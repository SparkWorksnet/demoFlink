package net.sparkworks.serialization;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class SensorDataSerializationSchema implements SerializationSchema<SensorData> {

    public byte[] serialize(final SensorData element) {
        return String.format("%s,%f,%d", element.getUrn(), element.getValue(), element.getTimestamp()).getBytes();
    }
}
