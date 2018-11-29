package net.sparkworks.functions;

import net.sparkworks.model.SummaryResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by akribopo on 09/09/2018.
 */
public class SummaryProcessWindowFunction extends ProcessWindowFunction<SummaryResult, SummaryResult, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<SummaryResult> elements, Collector<SummaryResult> out) throws Exception {
        final SummaryResult sm = elements.iterator().next();
        sm.setStart(context.window().getStart());
        sm.setUrn(s);
        out.collect(sm);
    }
}
