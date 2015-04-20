package com.loadbalancers.traces;

import com.loadbalancers.balancer.LoadBalancer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */

public class ConstantTraceGenerator {
    private final static long interarrivalTime = 10;
    private final static long msPerJob = 500;
    private final static long traceDurationSeconds = 10 * 1000;

    public static void main (final String[] args) throws Exception {
        final LoadBalancer.Trace trace = makeTrace();
        System.out.println("Created trace.");
        final File traceOutFile = Paths.get("traces", trace.getTraceName()).toFile();
        traceOutFile.createNewFile();
        final FileOutputStream traceOutputFile = new FileOutputStream(traceOutFile);
        System.out.println("Made output file:\t" + traceOutFile.getName() + ".");

        trace.writeTo(traceOutputFile);
        System.out.println("Wrote trace.");
    }

    protected static LoadBalancer.Trace makeTrace() {
        long time = 0;
        LoadBalancer.Trace.Builder traceBuilder = LoadBalancer.Trace.newBuilder();
        traceBuilder.setTraceName("Constant Trace");
        while (time < traceDurationSeconds) {
            final LoadBalancer.TraceRequest.Builder traceReqBuilder = LoadBalancer.TraceRequest.newBuilder();
            traceReqBuilder.setInterarrivalDelay(interarrivalTime);

            final LoadBalancer.ClientRequest.Builder clientReqBuilder = LoadBalancer.ClientRequest.newBuilder();
            clientReqBuilder.setType(LoadBalancer.JobType.TIMED_JOB);
            clientReqBuilder.setSimulatedJobDuration(msPerJob);
            traceReqBuilder.setReq(clientReqBuilder);

            traceBuilder.addReqs(traceReqBuilder);
            time += interarrivalTime;
        }
        return traceBuilder.build();
    }
}
