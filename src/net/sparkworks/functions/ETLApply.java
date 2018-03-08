package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Vector;

/**
 * An implementation of an ETL using the Apply operator.
 *
 * <p>Note that this function requires that all data in the windows is buffered until the window
 * is evaluated, as the function provides no means of incremental aggregation.
 *
 * @author ichatz@gmail.com
 */
public class ETLApply
        implements WindowFunction<SensorData, SensorData, String, Window> {

    public void apply(String key, Window window, Iterable<SensorData> values, Collector<SensorData> out)
            throws Exception {
        List<SensorData> accepted = new Vector<SensorData>();

        int sum = 0;
        for (SensorData t : values) {
            sum += t.getValue();
            accepted.add(t);
        }

        for (SensorData t : accepted) {
            out.collect(t);
        }
    }

}
