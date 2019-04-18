package net.sparkworks.functions;

import net.sparkworks.model.OutliersResult;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OutliersDetectionDataAscendingTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<OutliersResult> {

    public OutliersDetectionDataAscendingTimestampExtractor() {
        super(Time.minutes(5));
    }

    @Override
    public long extractTimestamp(OutliersResult outliersResult) {
        return outliersResult.getTimestamp();
    }
}
