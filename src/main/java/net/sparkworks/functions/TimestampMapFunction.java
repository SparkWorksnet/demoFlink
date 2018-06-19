package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Calendar;

/**
 * Simple map function for converting sensor data and adjusting the timestamp to the start of the window.
 *
 * @author ichatz@gmail.com
 */
public class TimestampMapFunction implements MapFunction<SensorData, SensorData> {

    public final int DEFAULTWINDOWMINUTES = 2;

    public int windowMinutes;

    public TimestampMapFunction() {
        this.windowMinutes = DEFAULTWINDOWMINUTES;
    }

    public int getWindowMinutes() {
        return windowMinutes;
    }

    public void setWindowMinutes(final int windowMinutes) {
        this.windowMinutes = windowMinutes;
    }

    public SensorData map(SensorData value) throws Exception {
        Calendar local = Calendar.getInstance();
        local.setTimeInMillis(value.getTimestamp());
        local.set(Calendar.MILLISECOND, 0);
        local.set(Calendar.SECOND, 0);
        int minutes = local.get(Calendar.MINUTE) / getWindowMinutes();
        local.set(Calendar.MINUTE, getWindowMinutes() * minutes);
        value.setTimestamp(local.getTimeInMillis());
        return value;
    }
}

