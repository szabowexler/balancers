package com.loadbalancers.balancer.worker;

import com.loadbalancers.balancer.LoadBalancer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */

public class SimulationWorkerImpl extends WorkerImpl {
    private final static Logger log = LogManager.getLogger(SimulationWorkerImpl.class);

    protected void processWork (final Work w) {
        w.jobStartTime = System.currentTimeMillis();
        log.info("Worker " + workerID + " starting job " + w.request.getJobID() + ".");
        final long duration = w.request.getSimulatedJobDuration();
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ex) {}

        final LoadBalancer.BalancerResponse.Builder respBuilder = LoadBalancer.BalancerResponse.newBuilder();
        respBuilder.setJobID(w.request.getJobID());
        final LoadBalancer.BalancerResponse resp = respBuilder.build();
        w.jobEndTime = System.currentTimeMillis();

        final long timeOnQueue = w.jobStartTime - w.jobReceiveTime;
        final long timeForJob = w.jobEndTime - w.jobStartTime;
        log.info("Job " + w.request.getJobID() + " spent " + timeOnQueue + " ms in queue, and took " + timeForJob + " ms.");
        w.done.run(resp);
    }
}
