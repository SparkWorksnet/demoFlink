package net.sparkworks.functions;

import net.sparkworks.model.OutliersResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class OutlierTupleDataMapFunction implements MapFunction<OutliersResult, Tuple4<String, Long, Long, Long>> {

    public Tuple4<String, Long, Long, Long> map(OutliersResult outliersResult) {
        return Tuple4.of(outliersResult.getUrn(), outliersResult.getTimestamp(), outliersResult.getValuesCount(),
                outliersResult.getOutliersCount());
    }
}
