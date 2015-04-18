package com.loadbalancers.analysis.analyzers.balancer;


import com.loadbalancers.analysis.events.LogEvent;
import com.loadbalancers.analysis.events.LogEventStream;
import org.jfree.data.xy.DefaultTableXYDataset;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.data.xy.XYSeries;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */

@Component
public class DuplicateRateAnalyzer extends MasterAnalyzer{
    public final static long GRANULARITY_MS = 1000;
    public final static String TYPE_DUPLICATE_RATE_GRAPH_FILENAME = "TypeDuplicateArrivalRateGraph.png";
    public final static String TYPE_DUPLICATE_RATE_GRAPH_TITLE = "Duplicate Request Arrival Rates";
    public final static String TYPE_DUPLICATE_RATE_X_AXIS_LABEL = "Time (S)";
    public final static String TYPE_DUPLICATE_RATE_Y_AXIS_LABEL = "Duplicate Rate (Reqs / Second)";

    public DuplicateRateAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing duplicate typed arrival rates...");

        final TableXYDataset dataset = createDatasets(s);

        createStackedAreaChart(TYPE_DUPLICATE_RATE_GRAPH_TITLE, TYPE_DUPLICATE_RATE_X_AXIS_LABEL, TYPE_DUPLICATE_RATE_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, TYPE_DUPLICATE_RATE_GRAPH_FILENAME);

        System.out.println("Duplicate types arrival rate analysis completed.");
    }

    protected TableXYDataset createDatasets(final LogEventStream s) {
        final List<List<LogEvent>> eventsBucketed = s.bucket (GRANULARITY_MS);
        final List<List<LogEvent>> arrivalBuckets = eventsBucketed.parallelStream()
                .map(B ->
                        B.parallelStream()
                                .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST)
                                .collect(Collectors.toList()))
                .collect(Collectors.toList());
        final List<List<LogEvent>> cacheHitBuckets = eventsBucketed.parallelStream()
                .map(B ->
                        B.parallelStream()
                            .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_CACHE_HIT)
                            .collect(Collectors.toList()))
                .collect(Collectors.toList());

        DefaultTableXYDataset dataset = new DefaultTableXYDataset();

        for (LogEvent.JobType t : LogEvent.JobType.values()) {
            final List<List<LogEvent>> typeArrivalBuckets = getArrivalsByType(arrivalBuckets, t);
            final List<List<LogEvent>> typeCacheBuckets = getArrivalsByType(cacheHitBuckets, t);

            int total = typeArrivalBuckets.parallelStream()
                    .map(List::size)
                    .reduce(0, (s1, s2) -> s1 + s2);
            if (total == 0) continue;

            final HashSet<String> requestStrings = new HashSet<>();
            final XYSeries series = new XYSeries(t.toString(), false, false);
            double x = 0;
            for (int b = 0; b < typeArrivalBuckets.size(); b ++) {
                final List<LogEvent> arrivalBucket = typeArrivalBuckets.get(b);
                final List<LogEvent> cacheBucket = typeCacheBuckets.get(b);
                int count = 0;
                for (final LogEvent e : arrivalBucket) {
                    final String cmd = e.getJobCommand().get();
                    if (requestStrings.contains(cmd)) {
                        count++;
                    } else {
                        requestStrings.add(cmd);
                    }
                }
                count += cacheBucket.size();

                double y = count / (GRANULARITY_MS / 1000.);
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
