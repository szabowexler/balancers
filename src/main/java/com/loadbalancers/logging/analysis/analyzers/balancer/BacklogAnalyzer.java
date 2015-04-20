package com.loadbalancers.logging.analysis.analyzers.balancer;


import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */

@Component
public class BacklogAnalyzer extends MasterAnalyzer {
    public final static String BACKLOG_GRAPH_FILENAME = "BacklogGraph.png";
    public final static String BACKLOG_GRAPH_TITLE = "Number Outstanding Requests";
    public final static String BACKLOG_X_AXIS_LABEL = "Time (S)";
    public final static String BACKLOG_Y_AXIS_LABEL = "# Outstanding Requests";

    @Override
    public void analyze(final LogEventStream s) {
        System.out.println("Analyzing backlog...");

        final XYSeries series = createData(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);

        createLinePlotPNG(BACKLOG_GRAPH_TITLE, BACKLOG_X_AXIS_LABEL, BACKLOG_Y_AXIS_LABEL,
                dataset, BALANCER_SUBFOLDER, BACKLOG_GRAPH_FILENAME);

        System.out.println("Backlog analysis completed.");
    }

    protected XYSeries createData (final LogEventStream s) {
        final XYSeries series = new XYSeries("Backlog");
        final Iterator<Logs.LogEvent> logIterator = s.iterator();
        final Set<Integer> outstandingRequests = new HashSet<>();
        long nextTickMS = s.getStreamStart();

        while (logIterator.hasNext()) {
            final Logs.LogEvent next = logIterator.next();
            while (next.getTime() > nextTickMS) {
                series.add((double) nextTickMS / 1000., (double) outstandingRequests.size());
                nextTickMS += DEFAULT_GRANULARITY_MS;
            }

            if (next.getEventType() == Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST) {
                outstandingRequests.add(next.getJobID());
            } else if (next.getEventType() == Logs.LogEventType.SERVER_EVENT_RECEIVE_WORKER_RESPONSE) {
                outstandingRequests.remove(next.getJobID());
            }
        }
        return series;
    }
}
