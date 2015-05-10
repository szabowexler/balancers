package com.loadbalancers.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public class LogEventStream {
    private final static Logger log = LogManager.getLogger(LogEventStream.class);
    protected List<Logs.LogEvent> events;

    public LogEventStream() {
        this.events = new ArrayList<>();
    }

    public LogEventStream(List<Logs.LogEvent> events) {
        this.events = events;
    }

    public ListIterator<Logs.LogEvent> iterator() {
        return events.listIterator();
    }

    public Logs.LogEvent get(int index) {
        return events.get(index);
    }

    public void setStreamStart(final long newStart) {
        final long delta = newStart - events.get(0).getTime();
        events = events.stream().map(e -> e.toBuilder().setTime(e.getTime() + delta).build())
                .collect(Collectors.toList());
    }

    public Logs.LogEvent getClosestEvent (final long t) {
        return events.stream()
                .reduce((e1, e2) -> Math.abs(e1.getTime() - t) < Math.abs(e2.getTime() - t) ? e1 : e2).get();
    }

    public long getStreamStart () {
        return events.get(0).getTime();
    }

    public long getStreamEnd () {
        return events.get(events.size() - 1).getTime();
    }

    public long getStreamDurationMS () {
        return getStreamEnd() - getStreamStart();
    }

    public long getTimeSinceStartMS (final Logs.LogEvent le) {
        return le.getTime() - getStreamStart();
    }

    public LogEventStream filterForType (Logs.LogEventType t) {
        final List<Logs.LogEvent> tEvents = events.parallelStream().filter(e -> e.getEventType() == t).collect(Collectors.toList());
        return new LogEventStream(tEvents);
    }



    public List<List<Logs.LogEvent>> bucket (long msPerBucket) {
        long totalMS = getStreamDurationMS();
        long numBuckets = totalMS / msPerBucket + 1;
        if (totalMS % msPerBucket != 0) {
            numBuckets ++;
        }

        final ArrayList<List<Logs.LogEvent>> buckets = new ArrayList<>();
        for (int b = 0; b < numBuckets; b++) {
            buckets.add(new ArrayList<>());
        }

        events.forEach(e -> {
            final long eventTime = e.getTime();
            final long timeFromStart = eventTime - getStreamStart();
            final int bucket = (int) (timeFromStart / msPerBucket);
            buckets.get(bucket).add(e);
        });

        return buckets;
    }

    public List<Logs.LogEvent> getEventsForWorker (final int workerID) {
        return events.parallelStream()
                .filter(Logs.LogEvent::hasWorkerID)
                .filter(e -> e.getWorkerID() == workerID)
                .collect(Collectors.toList());
    }

    public List<Logs.LogEvent> getEventsByJobID (final int jobID) {
        return events.parallelStream()
                .filter(Logs.LogEvent::hasJobID)
                .filter(e -> e.getJobID() == jobID)
                .collect(Collectors.toList());
    }

    public int size () {
        return events.size();
    }

    public Optional<Integer> getWorkerID() {
        final List<Logs.LogEvent> workerBootEvents = events.stream()
                .filter(e -> e.getEventType() == Logs.LogEventType.WORKER_EVENT_BOOTED)
                .collect(Collectors.toList());
        if (workerBootEvents.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(workerBootEvents.get(0).getWorkerID());
        }
    }

    public List<LogEventStream> extractWorkerStreams() {
        final List<Logs.LogEvent> workerEvents = events.stream().filter(e ->
                        e.getEventType().getNumber() >= 100
        ).collect(Collectors.toList());
        final Map<Integer, List<Logs.LogEvent>> workerEventStreams =
                workerEvents.stream().collect(Collectors.groupingBy(Logs.LogEvent::getWorkerID));

        final ArrayList<LogEventStream> streams = new ArrayList<>();
        workerEventStreams.values().forEach(S -> streams.add(new LogEventStream(S)));
        return streams;
    }

    public LogEventStream extractMasterStream () {
        final List<Logs.LogEvent> masterEvents = events.stream().filter(e ->
                        e.getEventType().getNumber() < 100
        ).collect(Collectors.toList());

        return new LogEventStream(masterEvents);
    }

    public String toString () {
        if (events.size() == 0) {
            return "<EMPTY STREAM>";
        } else {
            return "<" + events.size() + " events:" + (getStreamEnd() - getStreamStart()) / 1000. + " s>";
        }
    }
}
