package net.sparkworks.functions;

import net.sparkworks.model.SensorData;
import net.sparkworks.model.SummaryResult;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Created by akribopo on 09/09/2018.
 */
public class SummaryAggregateFunction implements AggregateFunction<SensorData, SummaryAccumulator, SummaryResult> {

    @Override
    public SummaryAccumulator createAccumulator() {
        return new SummaryAccumulator();
    }

    @Override
    public SummaryAccumulator add(SensorData value, SummaryAccumulator accumulator) {
        accumulator.addValue(value.getValue());
        return accumulator;
    }

    @Override
    public SummaryResult getResult(SummaryAccumulator accumulator) {
        final SummaryResult summaryResult = new SummaryResult();
        summaryResult.setMax(accumulator.getMax());
        summaryResult.setMin(accumulator.getMin());
        summaryResult.setSum(accumulator.getSum());
        summaryResult.setCount(accumulator.getCount());
        summaryResult.setAverage(accumulator.getSum() / accumulator.getCount());
        return summaryResult;
    }

    @Override
    public SummaryAccumulator merge(SummaryAccumulator a, SummaryAccumulator b) {
        return a.merge(b);
    }
}
