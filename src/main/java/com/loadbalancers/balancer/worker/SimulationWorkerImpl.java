package com.loadbalancers.balancer.worker;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.loadbalancers.balancer.LoadBalancer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */

public class SimulationWorkerImpl implements LoadBalancer.LoadBalancerWorker.Interface{
    private final static Logger log = LogManager.getLogger(SimulationWorkerImpl.class);
    protected int workerID = -1;

    public int getWorkerID() {
        return workerID;
    }

    @Override
    public void setID(final RpcController controller,
                      final LoadBalancer.BalancerConfigurationRequest request,
                      final RpcCallback<LoadBalancer.BalancerConfigurationResponse> done) {
        this.workerID = request.getWorkerID();
        LoadBalancer.BalancerConfigurationResponse.Builder builder = LoadBalancer.BalancerConfigurationResponse.newBuilder();
        builder.setAccepted(true);
        done.run(builder.build());
    }

    @Override
    public void doWork(final RpcController controller,
                       final LoadBalancer.BalancerRequest request,
                       final RpcCallback<LoadBalancer.BalancerResponse> done) {
        log.info("Worker " + workerID + " received message:\t" + request + ".");

        final long duration = request.getSimulatedJobDuration();
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ex) {}

        log.info("Job " + request.getJobID() + " took " + duration + " ms.");

        final LoadBalancer.BalancerResponse.Builder respBuilder = LoadBalancer.BalancerResponse.newBuilder();
        respBuilder.setJobID(request.getJobID());
        final LoadBalancer.BalancerResponse resp = respBuilder.build();
        done.run(resp);
    }
}
