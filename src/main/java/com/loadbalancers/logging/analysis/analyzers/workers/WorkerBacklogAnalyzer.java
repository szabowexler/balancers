package com.loadbalancers.logging.analysis.analyzers.workers;

import com.loadbalancers.logging.analysis.analyzers.system.NetworkLatencyAnalyzer;
import com.loadbalancers.logging.analysis.events.LogEvent;
import com.loadbalancers.logging.LogEventStream;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYSplineRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author Elias Szabo-Wexler
 * @since 24/March/2015
 */

@Component
public class WorkerBacklogAnalyzer extends WorkersAnalyzer{
    public final static long GRANULARITY_MS = 1000;
    public final static String WORKERS_BACKLOG_GRAPH_FILENAME = "WorkerBacklogGraph.png";
    public final static String WORKERS_BACKLOG_GRAPH_TITLE = "Number Outstanding Requests";
    public final static String WORKERS_BACKLOG_X_AXIS_LABEL = "Time (S)";
    public final static String WORKERS_BACKLOG_Y_AXIS_LABEL = "# Outstanding Requests";

    public WorkerBacklogAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final List<LogEventStream> s) {
        System.out.println("Analyzing worker backlog...");
        final Map<Integer, LogEventStream> workerIDToStreamMap = NetworkLatencyAnalyzer.makeWorkerToStreamMap(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        workerIDToStreamMap.forEach((id, S) -> dataset.addSeries(createData(id, S)));

        createLinePlotPNG(WORKERS_BACKLOG_GRAPH_TITLE, WORKERS_BACKLOG_X_AXIS_LABEL, WORKERS_BACKLOG_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, WORKERS_BACKLOG_GRAPH_FILENAME);
        ((XYPlot) chart.getPlot()).setRenderer(new XYSplineRenderer());

        System.out.println("Worker backlog analysis completed.");
    }

    protected XYSeries createData (final int workerID, final LogEventStream s) {
        final XYSeries series = new XYSeries("W" + workerID);
        final Iterator<LogEvent> logIterator = s.iterator();
        final Set<Integer> outstandingTags = new HashSet<>();
        long nextTickMS = s.getStreamStart();

        while (logIterator.hasNext()) {
            final LogEvent next = logIterator.next();
            while (next.getTime() > nextTickMS) {
                series.add((double) nextTickMS / 1000., (double) outstandingTags.size());
                nextTickMS += GRANULARITY_MS;
            }

            if (next.getLogEventType() == LogEvent.EventType.WORKER_RECEIVE_TASK) {
                outstandingTags.add(next.getTag().get());
            } else if (next.getLogEventType() == LogEvent.EventType.WORKER_RETURN_TASK) {
                outstandingTags.remove(next.getTag().get());
            }
        }
        return series;
    }
}
