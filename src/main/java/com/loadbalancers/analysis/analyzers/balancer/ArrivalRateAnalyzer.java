package com.loadbalancers.analysis.analyzers.balancer;

import main.events.LogEvent;
import main.events.LogEventStream;
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
public class ArrivalRateAnalyzer extends MasterAnalyzer {
    public final static long GRANULARITY = 250L;
    public final static String DISPATCH_GRAPH_FILENAME = "DispatchGraph.png";
    public final static String DISPATCH_GRAPH_TITLE = "Request Arrival Rate";
    public final static String DISPATCH_X_AXIS_LABEL = "Time Since Start (S)";
    public final static String DISPATCH_Y_AXIS_LABEL = "Rate (Reqs / Second)";

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing dispatches...");

        final List<List<LogEvent>> buckets = s.bucket (GRANULARITY);
        final List<Integer> dataPoints =
                buckets.stream()
                        .map(b -> (int) b.stream()
                                .filter(e -> e.getLogEventType() == LogEvent.EventType.SERVER_DISPATCH_REQUEST)
                                .count()).collect(Collectors.toList());

        final XYSeries series = createData(dataPoints);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);

        createLinePlotPNG(DISPATCH_GRAPH_TITLE, DISPATCH_X_AXIS_LABEL, DISPATCH_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, DISPATCH_GRAPH_FILENAME);

        System.out.println("Dispatch analysis completed.");
    }

    protected XYSeries createData (final List<Integer> bucketedArrivals) {
        double timeMS = 0;
        final XYSeries series = new XYSeries("Dispatch");
        for (int i = 0; i < bucketedArrivals.size(); i ++) {
            double y = bucketedArrivals.get(i) / (GRANULARITY / 1000.);
            series.add(timeMS, y);
            timeMS += (GRANULARITY / 1000.);
        }
        return series;
    }
}
