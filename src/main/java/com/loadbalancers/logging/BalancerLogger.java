package com.loadbalancers.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 02/May/2015
 */
public class BalancerLogger extends EventLogger{
    private final static Logger log = LogManager.getLogger(BalancerLogger.class);

    public static void logBalancerType (final Logs.BalancerType t) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_SET_BALANCER_TYPE);
        eventBuilder.setBType(t);

        logEvent(eventBuilder);
    }

    public static void logWorkerBooted (final int workerID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_WORKER_BOOTED);

        logEvent(eventBuilder);
    }

    public static void logWorkerDied (final int workerID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_WORKER_DIED);

        logEvent(eventBuilder);
    }

    public static void logReceivedJob(final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setJobID(jobID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_RECEIVE_CLIENT_REQUEST);

        logEvent(eventBuilder);
    }

    public static void logSendJobResponse(final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setJobID(jobID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_SEND_CLIENT_RESPONSE);

        logEvent(eventBuilder);
    }

    public static void logSentWorkerReq(final int workerID, final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_SEND_WORKER_REQUEST);

        logEvent(eventBuilder);
    }

    public static void logReceivedWorkerResp(final int workerID, final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);
        eventBuilder.setEventType(Logs.LogEventType.SERVER_EVENT_RECEIVE_WORKER_RESPONSE);

        logEvent(eventBuilder);
    }
}

