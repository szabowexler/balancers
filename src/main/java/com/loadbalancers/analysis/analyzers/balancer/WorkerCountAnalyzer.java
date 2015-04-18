package com.loadbalancers.analysis.analyzers.balancer;

import main.events.LogEvent;
import main.events.LogEventStream;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */

@Component
public class WorkerCountAnalyzer extends MasterAnalyzer {
    public final static long WORKER_COUNT_GRANULARITY_MS = DEFAULT_GRANULARITY_MS;
    public final static String WORKER_COUNT_GRAPH_FILENAME = "WorkerCountGraph.png";
    public final static String WORKER_COUNT_GRAPH_TITLE = "Number Living Workers";
    public final static String WORKER_COUNT_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_COUNT_Y_AXIS_LABEL = "Number Workers";

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing worker counts...");

        final XYSeries series = createData(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);

        createLinePlotPNG(WORKER_COUNT_GRAPH_TITLE, WORKER_COUNT_X_AXIS_LABEL, WORKER_COUNT_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, WORKER_COUNT_GRAPH_FILENAME);

        System.out.println("Worker counts analysis completed.");
    }

    protected XYSeries createData (final LogEventStream s) {
        final XYSeries series = new XYSeries("Worker Counts");
        final Iterator<LogEvent> logIterator = s.iterator();
        final Set<Integer> liveWorkers = new HashSet<>();
        long nextTickMS = s.getStreamStart();

        while (logIterator.hasNext()) {
            final LogEvent next = logIterator.next();
            while (next.getTime() > nextTickMS) {
                series.add((double) nextTickMS / 1000., (double) liveWorkers.size());
                nextTickMS += WORKER_COUNT_GRANULARITY_MS;
            }

            if (next.getLogEventType() == LogEvent.EventType.SERVER_WORKER_BOOTED) {
                liveWorkers.add(next.getWorkerID().get());
            } else if (next.getLogEventType() == LogEvent.EventType.SERVER_KILL_WORKER) {
                liveWorkers.remove(next.getWorkerID().get());
            }
        }
        return series;
    }
}
