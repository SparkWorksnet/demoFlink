package net.sparkworks.reporter;

import org.apache.commons.io.FileUtils;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class FileReporter extends AbstractReporter implements Scheduled {

    private String lineSeparator = System.lineSeparator();

    private final List<Function<String, Boolean>> orderConditions = Arrays.asList(
            s1 -> s1.contains("CPU"),
            s1 -> s1.contains("Heap"),
            s1 -> s1.contains("Split Reader: Custom File Source") && !s1.contains("Map"),
            s1 -> s1.contains("Split Reader: Custom File Source") && s1.contains("Map"),
            s1 -> s1.contains("Timestamps/Watermarks") && !s1.contains("Map"),
            s1 -> s1.contains("TumblingEventTimeWindows"),
            s1 -> s1.contains("Processor_Map")
    );

    @Override
    public String filterCharacters(String s) {
        return s;
    }

    @Override
    public void open(MetricConfig metricConfig) {
    }

    @Override
    public void close() {

    }

    @Override
    public void report() {
        final long timestamp = Instant.now().toEpochMilli();
        final File file = new File("/tmp/results.csv");
        final StringBuilder header = new StringBuilder(String.valueOf("#" + timestamp));
        final StringBuilder values = new StringBuilder(String.valueOf(timestamp));
        AtomicBoolean hasMetrics = new AtomicBoolean(false);

        orderConditions.forEach(condition -> {
            this.gauges.forEach((gauge, gaugeName) -> {
                if ((gaugeName.contains("Status_JVM_CPU") ||
                        gaugeName.contains("Heap_Used") ||
                        gaugeName.contains("NonHeap_Used") ||
                        gaugeName.contains("System_CPU") || gaugeName.contains("System_Memory") || gaugeName.contains("System_Swap")) &&
                        !gaugeName.contains("jobmanager") && condition.apply(gaugeName)) {
                    header.append("@" + gaugeName);
                    values.append("@" + gauge.getValue());
                    hasMetrics.set(true);
                }
            });
            this.meters.forEach((meter, meterName) -> {
                if ((meterName.contains("numRecords") || meterName.contains("System_CPU") || meterName.contains("System_Memory") || meterName.contains("System_Swap")) &&
                        !meterName.contains("jobmanager") && condition.apply(meterName)) {
                    header.append("@" + meterName);
                    values.append("@" + meter.getRate());
                    hasMetrics.set(true);
                }
            });
            this.counters.forEach((counter, counterName) -> {
                if ((counterName.contains("numRecords") || counterName.contains("System_CPU") || counterName.contains("System_Memory") || counterName.contains("System_Swap")) &&
                        !counterName.contains("jobmanager") && condition.apply(counterName)) {
                    header.append("@" + counterName);
                    values.append("@" + counter.getCount());
                    hasMetrics.set(true);
                }
            });
        });
        if (Objects.nonNull(file) && hasMetrics.get()) {
            try {
                FileUtils.writeStringToFile(file, header.toString() + lineSeparator + values.toString() + lineSeparator, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
