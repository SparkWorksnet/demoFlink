package net.sparkworks.reporter;

import org.apache.commons.io.FileUtils;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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
        this.meters.forEach((meter, s) -> {
            if (s.contains("throughput")) {
                String filename = s.substring(s.lastIndexOf("-") + 1);
                final File file = Paths.get("/tmp/" + filename + ".txt").toFile();
                try {
                    FileUtils.writeStringToFile(file, lineSeparator, true);
                    FileUtils.writeStringToFile(file, s + ": " + meter.getRate(), true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
    
                this.gauges.forEach((gauge, gaugeName) -> {
                    if (gaugeName.contains("CPU")) {
                        try {
                            FileUtils.writeStringToFile(file, lineSeparator, true);
                            FileUtils.writeStringToFile(file, gaugeName + ": " + gauge.getValue(), true);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });
    }
}
