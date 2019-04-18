package net.sparkworks.functions;

import net.sparkworks.model.OutliersResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OutliersDetectProcessWindowFunction extends ProcessWindowFunction<OutliersResult, OutliersResult, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<OutliersResult> elements, Collector<OutliersResult> out) {
        final OutliersResult sm = elements.iterator().next();
        sm.setTimestamp(context.window().getStart());
        sm.setUrn(s);
        out.collect(sm);
    }
}
