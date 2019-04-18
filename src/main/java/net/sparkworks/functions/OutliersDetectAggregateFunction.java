package net.sparkworks.functions;

import net.sparkworks.model.OutliersResult;
import net.sparkworks.model.FlaggedSensorData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OutliersDetectAggregateFunction implements AggregateFunction<FlaggedSensorData, OutliersDetectAccumulator, OutliersResult> {

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
    public OutliersResult getResult(OutliersDetectAccumulator accumulator) {
        final OutliersResult outliersResult = new OutliersResult();
        outliersResult.setValuesCount(accumulator.getCount());
        outliersResult.setOutliersCount(accumulator.getOutlierCount());
        return outliersResult;
    }

    @Override
    public OutliersDetectAccumulator merge(OutliersDetectAccumulator a, OutliersDetectAccumulator b) {
        return a.merge(b);
    }
}
