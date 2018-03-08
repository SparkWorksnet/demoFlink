package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Simple map function for converting RabbitMQ Message payloads to SensorData objects.
 *
 * @author ichatz@gmail.com
 */
public class SensorDataMapFunction implements MapFunction<String, SensorData> {

    public SensorData map(String message) throws Exception {
        final String[] items = message.split(",");

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
