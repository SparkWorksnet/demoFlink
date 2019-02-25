package net.sparkworks.functions;

import net.sparkworks.model.CountersResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OutliersDetectProcessWindowFunction extends ProcessWindowFunction<CountersResult, CountersResult, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<CountersResult> elements, Collector<CountersResult> out) {
        final CountersResult sm = elements.iterator().next();
        sm.setTimestamp(context.window().getStart());
        sm.setUrn(s);
        out.collect(sm);
    }
}
