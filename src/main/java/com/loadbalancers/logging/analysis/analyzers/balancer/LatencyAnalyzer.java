package com.loadbalancers.logging.analysis.analyzers.balancer;


import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */

@Component
public class LatencyAnalyzer extends MasterAnalyzer{
    public final static String LATENCY_GRAPH_FILENAME = "LatencyGraph.png";
    public final static String LATENCY_GRAPH_TITLE ="Average Request Latency";
    public final static String LATENCY_X_AXIS_LABEL = "Time Since Start (S)";
    public final static String LATENCY_Y_AXIS_LABEL = "Average Latency Since Start (ms)";

    protected Set<Integer> hiddenSeries;
    protected XYSeriesCollection dataset;

    public LatencyAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing latency...");

        dataset = new XYSeriesCollection();
        final XYSeries series = createData(s);
        hiddenSeries = new HashSet<>();
        dataset.addSeries(series);

        createLinePlotPNG(LATENCY_GRAPH_TITLE, LATENCY_X_AXIS_LABEL, LATENCY_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, LATENCY_GRAPH_FILENAME);


        System.out.println("Latency analysis completed.");
    }

    protected XYSeries createData (final LogEventStream s) {
        final List<List<Logs.LogEvent>> firstBucketing = s.bucket(DEFAULT_GRANULARITY_MS);
        final List<List<Logs.LogEvent>> buckets = firstBucketing.parallelStream()
                .map(B -> B.stream()
                        .filter(e -> e.getEventType() == Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());

        final List<Long> bucketedLatencies = createBucketedLatencies(s, buckets);

        final XYSeries series = new XYSeries("Overall");
        addDatapoints(series, buckets, bucketedLatencies);
        return series;
    }

    protected final void addDatapoints (final XYSeries series,
                                        final List<List<Logs.LogEvent>> buckets, final List<Long> bucketedLatencies) {
        int count = 0;
        long sum = 0;
        for (int b = 0; b < buckets.size(); b ++) {
            int bucketSize = buckets.get(b).size();
            if (bucketSize == 0) continue;
            count += bucketSize;
            sum += bucketedLatencies.get(b);

            double averageLatency = (double) sum / (double) count;
            double time = (b * DEFAULT_GRANULARITY_MS) / 1000.;
            series.add(time, averageLatency);
        }
    }

    protected final List<Long> createBucketedLatencies (final LogEventStream s, final List<List<Logs.LogEvent>> buckets) {
        return buckets.stream().map(B -> B.parallelStream()
                .filter(e -> e.getEventType() == Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST)
                .map(e -> {
                    int jobID = e.getJobID();
                    final List<Logs.LogEvent> eventsForTag = s.getEventsByJobID(jobID);
                    final Logs.LogEvent response = eventsForTag.stream()
                            .filter(ev -> ev.getEventType() == Logs.LogEventType.SERVER_EVENT_RECEIVE_WORKER_RESPONSE)
                            .collect(Collectors.toList())
                            .get(0);
                    return response.getTime() - e.getTime();
                }).collect(Collectors.summingLong(x -> x)))
                .collect(Collectors.toList());
    }
}
