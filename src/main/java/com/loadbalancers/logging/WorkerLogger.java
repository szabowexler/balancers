package com.loadbalancers.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author Elias Szabo-Wexler
 * @since 19/April/2015
 */
public class WorkerLogger {
    private final static Logger log = LogManager.getLogger(WorkerLogger.class);
    protected static int workerID = -1;

    public static void setWorkerID (final int id) {
        workerID = id;
    }

    public static void logBooted () {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_BOOTED);

        logEvent(eventBuilder);
    }

    public static void logReceivedRequest (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_RECEIVE_REQUEST);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public static void logStartTask (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_START_TASK);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public static void logFinishTask (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_FINISH_TASK);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public static void logSendResponse (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_SEND_RESPONSE);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public static void logLoad (final Logs.LoadSnapshot load) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_REPORT_LOAD);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setLoad(load);

        logEvent(eventBuilder);
    }

    protected static void logEvent (final Logs.LogEvent.Builder builder) {
        builder.setTime(System.currentTimeMillis());
        final Logs.LogEvent e = builder.build();
        final ByteArrayOutputStream str = new ByteArrayOutputStream();
        try {
            e.writeTo(str);
            log.info("LOG_EVENT:" + str.toString());
        } catch (IOException ex) {
            log.error("Failed to write log event to string!", ex);
        }
    }
}
