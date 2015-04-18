package com.loadbalancers.analysis.analyzers.balancer;


import com.loadbalancers.analysis.events.LogEvent;
import com.loadbalancers.analysis.events.LogEventStream;
import org.jfree.chart.renderer.AbstractRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.awt.*;
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
        int addedSets = 0;
        dataset.addSeries(series);
        addedSets++;
        for (LogEvent.JobType t : LogEvent.JobType.values()) {
            final XYSeries typeLatency = createData(s, t);
            if (!typeLatency.isEmpty()) {
                dataset.addSeries(typeLatency);
                addedSets++;

                final long cutoff = t.getLatencyThreshold();
                final XYSeries bound = new XYSeries("");
                bound.add(0, cutoff);
                bound.add(s.getStreamEnd() / 1000., cutoff);
                dataset.addSeries(bound);
                hiddenSeries.add(addedSets);
                addedSets++;
            }
        }

        createLinePlotPNG(LATENCY_GRAPH_TITLE, LATENCY_X_AXIS_LABEL, LATENCY_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, LATENCY_GRAPH_FILENAME);


        System.out.println("Latency analysis completed.");
    }

    protected void configureChartBeforeSave () {
        final AbstractRenderer renderer = (AbstractRenderer) chart.getXYPlot().getRenderer();
        hiddenSeries.forEach(i -> renderer.setSeriesVisibleInLegend(i, false));
        final Stroke dashed = new BasicStroke(
                1.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                1.0f, new float[] {10.0f, 6.0f}, 0.0f );

        renderer.setSeriesPaint(0, Color.BLACK);
        for (int i = 1; i < dataset.getSeriesCount(); i+=2) {
            renderer.setSeriesPaint(i+1, renderer.lookupSeriesPaint(i));
            renderer.setSeriesStroke(i+1, dashed);
        }
    }

    protected XYSeries createData (final LogEventStream s) {
        final List<List<LogEvent>> firstBucketing = s.bucket(DEFAULT_GRANULARITY_MS);
        final List<List<LogEvent>> buckets = firstBucketing.parallelStream()
                .map(B -> B.stream()
                        .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());

        final List<Long> bucketedLatencies = createBucketedLatencies(s, buckets);

        final XYSeries series = new XYSeries("Overall");
        addDatapoints(series, buckets, bucketedLatencies);
        return series;
    }

    protected final void addDatapoints (final XYSeries series, final List<List<LogEvent>> buckets, final List<Long> bucketedLatencies) {
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

    protected final List<Long> createBucketedLatencies (final LogEventStream s, final List<List<LogEvent>> buckets) {
        return buckets.stream().map(B -> B.parallelStream()
                .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST)
                .map(e -> {
                    int tag = e.getTag().get();
                    final List<LogEvent> eventsForTag = s.getEventsForTag(tag);
                    final LogEvent response = eventsForTag.stream()
                            .filter(ev -> ev.getLogEventType() == LogEvent.EventType.SERVER_RECEIVE_RESPONSE)
                            .collect(Collectors.toList())
                            .get(0);
                    return response.getTime() - e.getTime();
                }).collect(Collectors.summingLong(x -> x)))
                .collect(Collectors.toList());
    }

    protected XYSeries createData (final LogEventStream s, final LogEvent.JobType t) {
        final List<List<LogEvent>> firstBucketing = s.bucket(DEFAULT_GRANULARITY_MS);
        final List<List<LogEvent>> buckets = firstBucketing.parallelStream()
                .map(B -> B.stream()
                        .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST && e.getJobType().get() == t)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
        final List<Long> bucketedLatencies = createBucketedLatencies(s, buckets);

        final XYSeries series = new XYSeries(t.toString());
        addDatapoints(series, buckets, bucketedLatencies);
        return series;
    }
}
