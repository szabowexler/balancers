package com.loadbalancers.logging;

/**
 * @author Elias Szabo-Wexler
 * @since 19/April/2015
 */
public class WorkerLogger extends EventLogger{
    protected int workerID = -1;
    protected boolean booted = false;

    public void setWorkerID (final int id) {
        workerID = id;
    }

    public void logBooted () {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setWorkerID(workerID);
        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_BOOTED);

        logEvent(eventBuilder);
        booted = true;
    }

    public  void logReceivedRequest (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_RECEIVE_REQUEST);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public  void logStartTask (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_START_TASK);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public  void logFinishTask (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_FINISH_TASK);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public  void logSendResponse (final int jobID) {
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_SEND_RESPONSE);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setJobID(jobID);

        logEvent(eventBuilder);
    }

    public  void logLoad (final Logs.LoadSnapshot load) {
        if (!booted) return;
        final Logs.LogEvent.Builder eventBuilder = Logs.LogEvent.newBuilder();

        eventBuilder.setEventType(Logs.LogEventType.WORKER_EVENT_REPORT_LOAD);
        eventBuilder.setWorkerID(workerID);
        eventBuilder.setLoad(load);

        logEvent(eventBuilder);
    }
}
