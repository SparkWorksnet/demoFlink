package net.sparkworks.serialization;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by akribopo on 09/09/2018.
 */
public class SensorDataDeserializationSchema extends AbstractDeserializationSchema<SensorData> {
    @Override
    public SensorData deserialize(byte[] message) throws IOException {
        final String[] items = new String(message, StandardCharsets.UTF_8).split(",");

        SensorData value = new SensorData();
        value.setUrn(items[0]);

        // extract value
        final String txtValue = items[1];
        try {
            final double doubleValue = Double.parseDouble(txtValue);

            value.setValue(doubleValue);

        } catch (Exception ex) {
            // either district does not exist or it is not an integer
            // simply ignore
        }

        // extract timestamp
        final String txtTS = items[2];
        try {
            final long longValue = Long.parseLong(txtTS);

            value.setTimestamp(longValue);

        } catch (Exception ex) {
            // either district does not exist or it is not an integer
            // simply ignore
        }

        return value;
    }
}
