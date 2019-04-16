package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class OutlierTupleDataMapFunction implements MapFunction<CountersResult, Tuple4<String, Long, Long, Long>> {

    public Tuple4<String, Long, Long, Long> map(CountersResult countersResult) {
        return Tuple4.of(countersResult.getUrn(), countersResult.getTimestamp(), countersResult.getValuesCount(),
                countersResult.getValuesCountOutliersCount());
    }
}
