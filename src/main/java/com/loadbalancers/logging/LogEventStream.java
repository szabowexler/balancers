package com.loadbalancers.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public class LogEventStream {
    protected final List<Logs.LogEvent> events;

    public LogEventStream() {
        this.events = new ArrayList<>();
    }

    public LogEventStream(List<Logs.LogEvent> events) {
        this.events = events;
    }

    public ListIterator<Logs.LogEvent> iterator() {
        return events.listIterator();
    }

    public void parse (final List<String> lines) {
        // TODO: parse a log file
    }

    public Logs.LogEvent get(int index) {
        return events.get(index);
    }

    public void setStreamStart(final long newStart) {
        final long delta = newStart - events.get(0).getTime();
        // TODO: implement time rescaling
        throw new UnsupportedOperationException("Not possible right now.");
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

    public String toString () {
        if (events.size() == 0) {
            return "<EMPTY STREAM>";
        } else {
            return "<" + events.size() + " events:" + (getStreamEnd() - getStreamStart()) / 1000. + " s>";
        }
    }
}
