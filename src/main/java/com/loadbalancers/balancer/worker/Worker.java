package com.loadbalancers.balancer.worker;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.logging.WorkerLogger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Elias Szabo-Wexler
 * @since 17/April/2015
 */
public abstract class Worker implements LoadBalancer.LoadBalancerWorker.Interface{
    private final static Logger log = LogManager.getLogger(Worker.class);
    protected int workerID = -1;

    protected final ThreadPoolExecutor threadPool;
    protected final LinkedBlockingQueue<Runnable> reqs;

    public Worker() {
        this.reqs = new LinkedBlockingQueue<>();
        final int logicalCores = Runtime.getRuntime().availableProcessors();
        this.threadPool = new ThreadPoolExecutor(logicalCores, logicalCores, 10, TimeUnit.SECONDS, reqs);
    }

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
        WorkerLogger.logBooted(log);
        done.run(builder.build());
    }

    @Override
    public void doWork(final RpcController controller,
                       final LoadBalancer.BalancerRequest request,
                       final RpcCallback<LoadBalancer.BalancerResponse> done) {
        log.info("Worker " + workerID + " received job:\t" + request.getJobID() + ".");
        WorkerLogger.logReceivedRequest(log, request.getJobID());
        final Work w = new Work(controller, request, done);
        threadPool.execute(w);
    }

    protected abstract void processWork (final Work w);


    class Work implements Runnable  {
        protected final RpcController controller;
        protected final LoadBalancer.BalancerRequest request;
        protected final RpcCallback<LoadBalancer.BalancerResponse> done;

        protected final long jobReceiveTime;
        protected long jobStartTime;
        protected long jobEndTime;

        public Work(final RpcController controller,
                    final LoadBalancer.BalancerRequest req,
                    final RpcCallback<LoadBalancer.BalancerResponse> done) {
            this.done = done;
            this.request = req;
            this.controller = controller;
            this.jobReceiveTime = System.currentTimeMillis();
        }

        @Override
        public void run() {
            processWork(this);
        }
    }
}
