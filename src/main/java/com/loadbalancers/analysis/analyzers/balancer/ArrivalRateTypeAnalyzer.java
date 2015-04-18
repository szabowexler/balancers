package com.loadbalancers.analysis.analyzers.balancer;


import com.loadbalancers.analysis.events.LogEvent;
import com.loadbalancers.analysis.events.LogEventStream;
import org.jfree.data.xy.DefaultTableXYDataset;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.data.xy.XYSeries;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */

@Component
public class ArrivalRateTypeAnalyzer extends MasterAnalyzer {
    public final static long GRANULARITY_MS = 1000L;
    public final static String TYPE_ARRIVAL_RATE_GRAPH_FILENAME = "TypeArrivalRateGraph.png";
    public final static String TYPE_ARRIVAL_RATE_GRAPH_TITLE = "Requests";
    public final static String TYPE_ARRIVAL_RATE_X_AXIS_LABEL = "Time (S)";
    public final static String TYPE_ARRIVAL_RATE_Y_AXIS_LABEL = "Total Arrived";

    public ArrivalRateTypeAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing typed arrival rates...");

        final TableXYDataset dataset = createDatasets(s);

        createStackedAreaChart(TYPE_ARRIVAL_RATE_GRAPH_TITLE, TYPE_ARRIVAL_RATE_X_AXIS_LABEL, TYPE_ARRIVAL_RATE_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, TYPE_ARRIVAL_RATE_GRAPH_FILENAME);

        System.out.println("Types arrival rate analysis completed.");
    }

    protected TableXYDataset createDatasets(final LogEventStream s) {
        final List<List<LogEvent>> eventsBucketed = s.bucket (GRANULARITY_MS);
        final List<List<LogEvent>> arrivalBuckets = eventsBucketed.parallelStream()
                .map(B ->
                        B.parallelStream()
                                .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST ||
                                            e.getLogEventType() == LogEvent.EventType.SERVER_CACHE_HIT)
                                .collect(Collectors.toList()))
                .collect(Collectors.toList());

        DefaultTableXYDataset dataset = new DefaultTableXYDataset();

        for (LogEvent.JobType t : LogEvent.JobType.values()) {
            final List<List<LogEvent>> buckets = getArrivalsByType(arrivalBuckets, t);
            int total = buckets.parallelStream()
                    .map(B -> B.size())
                    .reduce(0, (s1, s2) -> s1 + s2);
            if (total == 0) continue;

            final XYSeries series = new XYSeries(t.toString(), false, false);
            double x = 0;
            for (int i = 0; i < buckets.size(); i ++) {
                int count = buckets.get(i).size();
                double y = count;
                series.add(x / 1000, y);
                x += GRANULARITY_MS;
            }
            dataset.addSeries(series);
        }

        return dataset;
    }

    protected List<List<LogEvent>> getArrivalsByType (final List<List<LogEvent>> buckets, final LogEvent.JobType t) {
        return buckets.parallelStream()
                .map(B -> B.parallelStream()
                        .filter(e -> e.getJobType().get() == t)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
    }
}
