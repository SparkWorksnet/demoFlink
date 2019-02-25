package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class OutliersDetectionDataAscendingTimestampExtractor extends AscendingTimestampExtractor<CountersResult> {
    @Override
    public long extractAscendingTimestamp(CountersResult element) {
        return element.getTimestamp();
    }
}
