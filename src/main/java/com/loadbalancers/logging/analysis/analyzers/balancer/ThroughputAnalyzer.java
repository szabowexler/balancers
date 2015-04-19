package com.loadbalancers.logging.analysis.analyzers.balancer;

import com.loadbalancers.logging.analysis.events.LogEvent;
import com.loadbalancers.logging.LogEventStream;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
@Component
public class ThroughputAnalyzer extends MasterAnalyzer{
    public final static String THROUGHPUT_GRAPH_FILENAME = "ThroughputGraph.png";
    public final static String THROUGHPUT_GRAPH_TITLE = "System Throughput";
    public final static String THROUGHPUT_X_AXIS_LABEL = "Time Since Start (S)";
    public final static String THROUGHPUT_Y_AXIS_LABEL = "Throughput (Over " + DEFAULT_GRANULARITY_MS + "-ms-window)";
    public final static long THROUGHPUT_GRANULARITY_MS = 5000;

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing throughput...");

        final List<List<LogEvent>> unfilteredBuckets = s.bucket(THROUGHPUT_GRANULARITY_MS);
        final List<Integer> buckets = unfilteredBuckets.parallelStream()
                .map(B -> B.parallelStream()
                        .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_RECEIVE_RESPONSE)

                        .collect(Collectors.toList())
                        .size())
                .collect(Collectors.toList());

        final XYSeries series = createData(buckets);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);
        createLinePlotPNG(THROUGHPUT_GRAPH_TITLE, THROUGHPUT_X_AXIS_LABEL, THROUGHPUT_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, THROUGHPUT_GRAPH_FILENAME);

        System.out.println("Throughput analysis completed.");
    }

    protected XYSeries createData (final List<Integer> responseBucketSizes) {
        final XYSeries series = new XYSeries("Throughput");
        for (int b = 0; b < responseBucketSizes.size(); b ++) {
            int count = responseBucketSizes.get(b);
            double throughput = (double) count / (double) (THROUGHPUT_GRANULARITY_MS / 1000.);
            double time = (b * THROUGHPUT_GRANULARITY_MS) / 1000.;
            series.add(time, throughput);
        }
        return series;
    }
}
