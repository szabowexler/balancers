package com.loadbalancers.logging.analysis.analyzers.system;


import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */
@Component
public class NetworkLatencyAnalyzer extends GlobalAnalyzer{
    private final static Logger log = LogManager.getLogger(NetworkLatencyAnalyzer.class);
    public final static long NETWORK_LATENCY_GRANULARITY_MS = 1000;
    public final static String NETWORK_LATENCY_GRAPH_FILENAME = "NetworkLatencyGraph.png";
    public final static String NETWORK_LATENCY_GRAPH_TITLE ="Average Request Network Latency";
    public final static String NETWORK_LATENCY_X_AXIS_LABEL = "Time (S)";
    public final static String NETWORK_LATENCY_Y_AXIS_LABEL = "Latency (ms)";

    @Override
    public void analyze(List<LogEventStream> workerStreams, LogEventStream masterStream) {
        System.out.println("Analyzing network latency...");

        final List<XYSeries> series = createDatasets(workerStreams, masterStream);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        createLinePlotPNG(NETWORK_LATENCY_GRAPH_TITLE, NETWORK_LATENCY_X_AXIS_LABEL, NETWORK_LATENCY_Y_AXIS_LABEL,
                dataset, SYSTEM_SUBFOLDER, NETWORK_LATENCY_GRAPH_FILENAME);

        System.out.println("Network latency analysis completed.");
    }

    protected List<XYSeries> createDatasets (final List<LogEventStream> workerStreams, final LogEventStream masterStream) {
        final LogEventStream dispatches = masterStream.filterForType(Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST);
        final ListIterator<Logs.LogEvent> iterator = dispatches.iterator();
        final Map<Integer, LogEventStream> workerIdToStreamMap = makeWorkerToStreamMap(workerStreams);

        final ArrayList<XYSeries> dataset = new ArrayList<>(workerStreams.size());
        final ArrayList<Set<Long>> networkTimes = new ArrayList<>();
        for (int i = 0; i < workerStreams.size(); i ++) {
            dataset.add(new XYSeries("W" + i));
            networkTimes.add(new HashSet<>());
        }
        long nextTickMS = 0;

        while (iterator.hasNext()) {
            final Logs.LogEvent d = iterator.next();
            while (masterStream.getTimeSinceStartMS(d) > nextTickMS) {
                for (int i = 0; i < workerStreams.size(); i ++) {
                    final LogEventStream wStream = workerStreams.get(i);
                    if (wStream.getStreamStart() <= nextTickMS && nextTickMS <= wStream.getStreamEnd()) {
                        final double avgLatency = networkTimes.get(i)
                                .stream()
                                .collect(Collectors.averagingDouble(Long::doubleValue));
                        dataset.get(i).add((double) nextTickMS / 1000., avgLatency);
                        networkTimes.get(i).clear();
                    }
                }
                nextTickMS += NETWORK_LATENCY_GRANULARITY_MS;
            }

            int jobID = d.getJobID();
            int dispatchWorkerId = d.getWorkerID();
            final LogEventStream stream = workerIdToStreamMap.get(dispatchWorkerId);

            final List<Logs.LogEvent> masterEvents = masterStream.getEventsByJobID(jobID);
            final List<Logs.LogEvent> workerEvents = stream.getEventsByJobID(jobID);
            final long networkTime = getMasterEventDuration(masterEvents) - getWorkerEventDuration(workerEvents);
            networkTimes.forEach(S -> S.add(networkTime));
        }
        return dataset;
    }

    public static Map<Integer, LogEventStream> makeWorkerToStreamMap (final List<LogEventStream> workerStreams) {
        final HashMap<Integer, LogEventStream> workerToStreamMap = new HashMap<>();

        workerStreams.forEach(s -> {
            final Optional<Integer> streamWorkerID = s.getWorkerID();
            if (streamWorkerID.isPresent()) {
                workerToStreamMap.put(streamWorkerID.get(), s);
            } else {
                log.error("Worker stream doesn't have worker ID.");
            }
        });

        return workerToStreamMap;
    }

    protected long getMasterEventDuration (final List<Logs.LogEvent> masterEvents) {
        return getEventDuration(masterEvents, Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST,
                Logs.LogEventType.SERVER_EVENT_RECEIVE_WORKER_RESPONSE);
    }

    protected long getWorkerEventDuration (final List<Logs.LogEvent> events) {
        return getEventDuration(events,
                Logs.LogEventType.WORKER_EVENT_RECEIVE_REQUEST,
                Logs.LogEventType.WORKER_EVENT_SEND_RESPONSE);
    }

    protected long getEventDuration(final List<Logs.LogEvent> events, final Logs.LogEventType start, final Logs.LogEventType end) {
        try {
            final Logs.LogEvent dispatchEvent = events.stream().filter(e -> e.getEventType() == start).collect(Collectors.toList()).get(0);
            final Logs.LogEvent responseEvent = events.stream().filter(e -> e.getEventType() == end).collect(Collectors.toList()).get(0);
            return responseEvent.getTime() - dispatchEvent.getTime();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
