package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import net.sparkworks.model.FlaggedSensorData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OutliersDetectAggregateFunction implements AggregateFunction<FlaggedSensorData, OutliersDetectAccumulator, CountersResult> {

    @Override
    public OutliersDetectAccumulator createAccumulator() {
        return new OutliersDetectAccumulator();
    }

    @Override
    public OutliersDetectAccumulator add(FlaggedSensorData value, OutliersDetectAccumulator accumulator) {
        accumulator.addValue();
        if (value.isOutlier()) accumulator.addOutlierValue();
        return accumulator;
    }

    @Override
    public CountersResult getResult(OutliersDetectAccumulator accumulator) {
        final CountersResult countersResult = new CountersResult();
        countersResult.setValuesCount(accumulator.getCount());
        countersResult.setValuesCountOutliersCount(accumulator.getOutlierCount());
        return countersResult;
    }

    @Override
    public OutliersDetectAccumulator merge(OutliersDetectAccumulator a, OutliersDetectAccumulator b) {
        return a.merge(b);
    }
}
