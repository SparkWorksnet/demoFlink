package net.sparkworks.reporter;

import org.apache.commons.io.FileUtils;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

public class FileReporter extends AbstractReporter implements Scheduled {
    
    private String lineSeparator = System.lineSeparator();
    
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
        this.gauges.forEach((gauge, gaugeName) -> {
            if ((gaugeName.contains("Status.JVM.CPU") ||
                    gaugeName.contains("Heap.Used") ||
                    gaugeName.contains("NonHeap.Used") ||
                    gaugeName.contains("System.CPU") || gaugeName.contains("System.Memory") || gaugeName.contains("System.Swap")) &&
                    !gaugeName.contains("jobmanager")) {
                header.append("@" + gaugeName.replace(".", "_"));
                values.append("@" + gauge.getValue());
            }
        });
        this.meters.forEach((meter, meterName) -> {
            if ((meterName.contains("numRecords") || meterName.contains("System.CPU")  || meterName.contains("System.Memory") || meterName.contains("System.Swap")) &&
                    !meterName.contains("jobmanager")) {
                header.append("@" + meterName.replace(".", "_"));
                values.append("@" + meter.getRate());
            }
        });
        this.counters.forEach((counter, counterName) -> {
            if ((counterName.contains("numRecords") || counterName.contains("System.CPU") || counterName.contains("System.Memory") || counterName.contains("System.Swap")) &&
                    !counterName.contains("jobmanager")) {
                header.append("@" + counterName.replace(".", "_"));
                values.append("@" + counter.getCount());
            }
        });
        if (Objects.nonNull(file)) {
            try {
                FileUtils.writeStringToFile(file, header.toString() + lineSeparator + values.toString() + lineSeparator, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
}
