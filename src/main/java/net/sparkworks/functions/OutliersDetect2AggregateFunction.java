package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import net.sparkworks.model.FlaggedCountersResult;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OutliersDetect2AggregateFunction implements AggregateFunction<FlaggedCountersResult, OutliersDetectAccumulator, CountersResult> {

    @Override
    public OutliersDetectAccumulator createAccumulator() {
        return new OutliersDetectAccumulator();
    }

    @Override
    public OutliersDetectAccumulator add(FlaggedCountersResult value, OutliersDetectAccumulator accumulator) {
        accumulator.addValue();
        if (value.isOutlier()) accumulator.addOutlierValue();
        return accumulator;
    }

    @Override
    public CountersResult getResult(OutliersDetectAccumulator accumulator) {
        final CountersResult countersResult = new CountersResult();
        countersResult.setCount(accumulator.getCount());
        countersResult.setOutliersCount(accumulator.getOutlierCount());
        return countersResult;
    }

    @Override
    public OutliersDetectAccumulator merge(OutliersDetectAccumulator a, OutliersDetectAccumulator b) {
        return a.merge(b);
    }
}