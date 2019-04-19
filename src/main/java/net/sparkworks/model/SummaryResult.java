package net.sparkworks.model;

import lombok.Setter;
import lombok.ToString;

/**
 * Created by akribopo on 09/09/2018.
 */
@Setter
@ToString
public class SummaryResult {

    private String urn;

    private long start;

    private double average;
    private double min;
    private double max;

    private long count;
    private double sum;
}
