package net.sparkworks.functions;

import net.sparkworks.model.FlaggedOutliersResult;
import net.sparkworks.model.OutliersResult;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OutliersOnOutliersDetectAggregateFunction implements AggregateFunction<FlaggedOutliersResult, OutliersDetectAccumulator, OutliersResult> {

    @Override
    public OutliersDetectAccumulator createAccumulator() {
        return new OutliersDetectAccumulator();
    }

    @Override
    public OutliersDetectAccumulator add(FlaggedOutliersResult value, OutliersDetectAccumulator accumulator) {
        accumulator.addValue();
        if (value.isOutlier()) accumulator.addOutlierValue();
        return accumulator;
    }

    @Override
    public OutliersResult getResult(OutliersDetectAccumulator accumulator) {
        final OutliersResult outliersResult = new OutliersResult();
        outliersResult.setOutliersCount(accumulator.getCount());
        outliersResult.setOutliersOnOutliersCount(accumulator.getOutlierCount());
        return outliersResult;
    }

    @Override
    public OutliersDetectAccumulator merge(OutliersDetectAccumulator a, OutliersDetectAccumulator b) {
        return a.merge(b);
    }
}
