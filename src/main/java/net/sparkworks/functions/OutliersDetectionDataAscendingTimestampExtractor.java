package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OutliersDetectionDataAscendingTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<CountersResult> {

    public OutliersDetectionDataAscendingTimestampExtractor() {
        super(Time.minutes(5));
    }

    @Override
    public long extractTimestamp(CountersResult countersResult) {
        return countersResult.getTimestamp();
    }
}
