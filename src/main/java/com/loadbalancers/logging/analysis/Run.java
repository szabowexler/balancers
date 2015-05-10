package com.loadbalancers.logging.analysis;

import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 08/May/2015
 */
public class Run {
    protected final LogEventStream masterStream;
    protected final List<LogEventStream> workerStreams;

    public Run(LogEventStream masterStream, List<LogEventStream> workerStreams) {
        this.masterStream = masterStream;
        this.workerStreams = workerStreams;
    }

    public LogEventStream getMasterStream() {
        return masterStream;
    }

    public List<LogEventStream> getWorkerStreams() {
        return workerStreams;
    }

    public String getBalancerType () {
        return masterStream.filterForType(Logs.LogEventType.SERVER_EVENT_SET_BALANCER_TYPE)
                .get(0)
                .getBType()
                .toString();
    }
}
