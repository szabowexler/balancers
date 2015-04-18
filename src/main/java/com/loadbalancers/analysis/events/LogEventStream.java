package com.loadbalancers.analysis.events;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public class LogEventStream {
    protected final List<LogEvent> events;

    public LogEventStream() {
        this.events = new ArrayList<>();
    }

    public LogEventStream(List<LogEvent> events) {
        this.events = events;
    }

    public ListIterator<LogEvent> iterator() {
        return events.listIterator();
    }

    public void parse (final List<String> lines) {
        lines.forEach(L -> {
            try {
                final int month = Integer.parseInt(L.substring(1, 3));
                final int day = Integer.parseInt(L.substring(3, 5));
                final String time = L.substring(6, 21);

                SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSSuuu");
                final String dateString = day + "/" + month + "/2015 " + time;
                final Date d;

                try {
                    d = format.parse(dateString);
                } catch (ParseException ex) {
                    throw new RuntimeException("Unable to parse: [" + dateString + "].");
                }

                final String logMessage = L.substring(L.indexOf("]") + 1).trim();
                for (LogEvent.EventType t : LogEvent.EventType.values()) {
                    if (t.matches(logMessage)) {
                        final LogEvent le = new LogEvent(t, d, logMessage);
                        events.add(le);
                        break;
                    }
                }
            } catch (NumberFormatException ex) {
                System.out.println("Skipping line:\t" + L);
            }
        });
        events.sort(Comparator.comparing(e -> e.time.getTime()));
    }

    public LogEvent get(int index) {
        return events.get(index);
    }

    public void setStreamStart(final long newStart) {
        final long delta = newStart - events.get(0).getTime();
        events.forEach(e -> e.addToTime(delta));
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

    public long getTimeSinceStartMS (final LogEvent le) {
        return le.getTime() - getStreamStart();
    }

    public LogEventStream filterForType (LogEvent.EventType t) {
        final List<LogEvent> tEvents = events.parallelStream().filter(e -> e.event == t).collect(Collectors.toList());
        return new LogEventStream(tEvents);
    }



    public List<List<LogEvent>> bucket (long msPerBucket) {
        long totalMS = getStreamDurationMS();
        long numBuckets = totalMS / msPerBucket + 1;
        if (totalMS % msPerBucket != 0) {
            numBuckets ++;
        }

        final ArrayList<List<LogEvent>> buckets = new ArrayList<>();
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

    public List<LogEvent> getEventsForTag (final int tag) {
        return events.parallelStream()
                .filter(LogEvent::hasTag)
                .filter(e -> e.getTag().get() == tag)
                .collect(Collectors.toList());
    }

    public List<LogEvent> getEventsForWorker (final int workerID) {
        return events.parallelStream()
                .filter(LogEvent::hasWorkerID)
                .filter(e -> e.getWorkerID().get() == workerID)
                .collect(Collectors.toList());
    }

    public int size () {
        return events.size();
    }

    public Optional<Integer> getWorkerID() {
        final List<LogEvent> workerBootEvents = events.stream()
                .filter(e -> e.getLogEventType() == LogEvent.EventType.WORKER_BOOT)
                .collect(Collectors.toList());
        if (workerBootEvents.isEmpty()) {
            return Optional.empty();
        } else {
            return workerBootEvents.get(0).getWorkerID();
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
