package com.loadbalancers.traces;

import com.loadbalancers.balancer.LoadBalancer;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.WeibullDistribution;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;

/**
 * @author Elias Szabo-Wexler
 * @since 17/April/2015
 */
public class RapGeniusTraceGenerator {
    private final static long traceDurationSeconds = 10 * 1000;

    /** Interarrival rate - mean requests/second */
    private final static double lambda = 150;

    /* Shape/scale paramters taken from Rap Genius's report */
    private final static double weibullShape = 0.46;
    private final static double weibullScale = 50 / Math.pow(Math.log(2), (1 / weibullShape));

    public static void main (final String[] args) throws Exception {
        final LoadBalancer.Trace trace = makeTrace();
        System.out.println("Created rap genius trace.");
        final File traceOutFile = Paths.get("traces", trace.getTraceName()).toFile();
        traceOutFile.createNewFile();
        final FileOutputStream traceOutputFile = new FileOutputStream(traceOutFile);
        System.out.println("Made output file:\t" + traceOutFile.getName() + ".");

        trace.writeTo(traceOutputFile);
        System.out.println("Wrote trace.");
    }

    protected static LoadBalancer.Trace makeTrace() {
        long time = 0;

        final ExponentialDistribution interarrivalGenerator = new ExponentialDistribution(lambda);
        final WeibullDistribution jobDurationGenerator = new WeibullDistribution(weibullShape, weibullScale);

        LoadBalancer.Trace.Builder traceBuilder = LoadBalancer.Trace.newBuilder();
        traceBuilder.setTraceName("Rap Genius Trace");
        while (time < traceDurationSeconds) {
            final long interarrivalTimeMillis = (long) interarrivalGenerator.sample();

            final long durationMillis = Long.min(30000, Long.max(10, (long) Math.ceil(jobDurationGenerator.sample())));

            final LoadBalancer.TraceRequest.Builder traceReqBuilder = LoadBalancer.TraceRequest.newBuilder();
            traceReqBuilder.setInterarrivalDelay(interarrivalTimeMillis);

            final LoadBalancer.ClientRequest.Builder clientReqBuilder = LoadBalancer.ClientRequest.newBuilder();
            clientReqBuilder.setType(LoadBalancer.JobType.TIMED_JOB);
            clientReqBuilder.setSimulatedJobDuration(durationMillis);
            traceReqBuilder.setReq(clientReqBuilder);

            traceBuilder.addReqs(traceReqBuilder);
            time += interarrivalTimeMillis;
        }
        return traceBuilder.build();
    }
}
