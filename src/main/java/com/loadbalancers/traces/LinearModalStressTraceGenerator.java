package com.loadbalancers.traces;

import com.loadbalancers.balancer.LoadBalancer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Random;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */

public class LinearModalStressTraceGenerator {
    private final static long MIN_INTERARRIVAL_TIME = 40;
    private final static long MAX_INTERARRIVAL_TIME = 1000;
    private final static long INTERARRIVAL_DELTA = 50;
    private final static int NUM_MAX_HITS_TO_BACKOFF = 25;
    private static long interarrivalTime = MAX_INTERARRIVAL_TIME;
    private final static long[] modes = new long [] {100, 500, 1000};
    private final static Random r = new Random(System.currentTimeMillis());
    private final static long traceDurationSeconds = 30 * 1000;

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
        traceBuilder.setTraceName("LinearModalIncreasingTrace");
        int numOnMaxLoad = 0;
        while (time < traceDurationSeconds) {
            final LoadBalancer.TraceRequest.Builder traceReqBuilder = LoadBalancer.TraceRequest.newBuilder();
            traceReqBuilder.setInterarrivalDelay(interarrivalTime);

            final LoadBalancer.ClientRequest.Builder clientReqBuilder = LoadBalancer.ClientRequest.newBuilder();
            clientReqBuilder.setType(LoadBalancer.JobType.TIMED_JOB);
            clientReqBuilder.setSimulatedJobDuration(nextMode());
            traceReqBuilder.setReq(clientReqBuilder);

            traceBuilder.addReqs(traceReqBuilder);
            time += interarrivalTime;
            interarrivalTime -= INTERARRIVAL_DELTA;
            if (interarrivalTime < MIN_INTERARRIVAL_TIME) {
                interarrivalTime = MIN_INTERARRIVAL_TIME;
                numOnMaxLoad++;
            }

            if (numOnMaxLoad > NUM_MAX_HITS_TO_BACKOFF) {
                numOnMaxLoad = 0;
                interarrivalTime = MAX_INTERARRIVAL_TIME;
            }
        }
        return traceBuilder.build();
    }

    protected static long nextMode () {
        final int index = r.nextInt(modes.length);
        return modes[index] + (long) ((r.nextDouble() - 0.5) * 50);
    }
}
