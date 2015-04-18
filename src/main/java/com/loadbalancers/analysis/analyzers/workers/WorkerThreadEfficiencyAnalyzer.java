package com.loadbalancers.analysis.analyzers.workers;


import com.loadbalancers.analysis.events.LogEvent;
import com.loadbalancers.analysis.events.LogEventStream;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */

@Component
public class WorkerThreadEfficiencyAnalyzer extends WorkersAnalyzer {
    public final static long WORKER_EFFICIENCY_GRANULARITY_MS = 100;
    public final static String WORKER_EFFICIENCY_GRAPH_FILENAME = "WorkerThreadEfficiencyGraph.png";
    public final static String WORKER_EFFICIENCY_GRAPH_TITLE = "Worker Thread Efficiency";
    public final static String WORKER_EFFICIENCY_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_EFFICIENCY_Y_AXIS_LABEL = "% Threads Working";

    protected int numThreads = 0;

    @Override
    public void analyze(final List<LogEventStream> s) {
        System.out.println("Analyzing worker efficiency...");

        final List<XYSeries> series = createDatasets(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        createLinePlotPNG(WORKER_EFFICIENCY_GRAPH_TITLE + "[" + numThreads + " threads]", WORKER_EFFICIENCY_X_AXIS_LABEL, WORKER_EFFICIENCY_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_EFFICIENCY_GRAPH_FILENAME);
        XYPlot plot = chart.getXYPlot();
        final NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setAutoRange(false);
        axis.setRange(0, 100);

        System.out.println("Worker thread analysis completed.");
    }

    protected List<XYSeries> createDatasets (final List<LogEventStream> s) {
        ArrayList<XYSeries> datasets = new ArrayList<>();

        for (int i = 0; i < s.size(); i ++) {
            datasets.add(createData(i, s.get(i)));
        }

        return datasets;
    }

    protected XYSeries createData (int i, final LogEventStream s) {
        final XYSeries series = new XYSeries("W" + i);
        numThreads = s.filterForType(LogEvent.EventType.WORKER_BOOT_THREAD).size();

        final List<List<LogEvent>> unfilteredBuckets = s.bucket(WORKER_EFFICIENCY_GRANULARITY_MS);
        final List<List<LogEvent>> buckets = unfilteredBuckets.parallelStream()
                .map(B -> B.parallelStream()
                        .filter(e -> e.getLogEventType() == LogEvent.EventType.WORKER_BLOCK_THREAD ||
                                e.getLogEventType() == LogEvent.EventType.WORKER_UNBLOCK_THREAD)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());

        final Iterator<LogEvent> logIterator = s.iterator();
        final Set<Integer> workingThreads = new HashSet<>();
        long nextTickMS = s.getStreamStart();

        while (logIterator.hasNext()) {
            final LogEvent next = logIterator.next();
            while (next.getTime() > nextTickMS) {
                final double numWorkingThreads = workingThreads.size();
                final double workingPercentage = numWorkingThreads / numThreads * 100.;
                final double time = nextTickMS / 1000.;

                series.add(time, workingPercentage);
                nextTickMS += WORKER_EFFICIENCY_GRANULARITY_MS;
            }

            if (next.getLogEventType() == LogEvent.EventType.WORKER_UNBLOCK_THREAD) {
                workingThreads.add(next.getTID().get());
            } else if (next.getLogEventType() == LogEvent.EventType.WORKER_BLOCK_THREAD) {
                workingThreads.remove(next.getTID().get());
            }
        }

        return series;
    }
}
