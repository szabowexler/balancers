package com.loadbalancers.balancer.impl;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.logging.BalancerLogger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 20/April/2015
 */
public abstract class DynamicLoadBalancerImpl extends LoadBalancerImpl {

    private final static Logger log = LogManager.getLogger(DynamicLoadBalancerImpl.class);

    public DynamicLoadBalancerImpl(final List<PeerInfo> servers,
                                   final String localHostname,
                                   final int localPort) {
        super(servers, localHostname, localPort);
    }

    @Override
    public void makeRequest(final RpcController controller,
                            final LoadBalancer.ClientRequest request,
                            final RpcCallback<LoadBalancer.ClientResponse> done) {
        final int workerID = newWorkerID();
        doRequest(controller, request, done, workerID);
    }

    protected void doRequest(final RpcController controller,
                             final LoadBalancer.ClientRequest request,
                             final RpcCallback<LoadBalancer.ClientResponse> done,
                             final int workerID) {
        try {
            log.info("Dynamic load balancer received message.");
            final RpcClientChannel randomServer = workerIdToChannelMap.get(workerID);
            final LoadBalancer.LoadBalancerWorker.Stub worker = LoadBalancer.LoadBalancerWorker.newStub(randomServer);
            final ClientRpcController workerController = randomServer.newRpcController();
            workerController.setTimeoutMs(0);

            final LoadBalancer.BalancerRequest balancerRequest = buildWorkerRequest(request);
            final int jobID = balancerRequest.getJobID();

            final RpcCallback<LoadBalancer.BalancerResponse> callback = res -> {
                onResponse(workerID, jobID);
                final LoadBalancer.ClientResponse.Builder respBuilder = LoadBalancer.ClientResponse.newBuilder();

                respBuilder.setResponse(res.getResponse());

                final LoadBalancer.ClientResponse resp = respBuilder.build();
                BalancerLogger.logSendJobResponse(jobID);
                done.run(resp);
            };

            log.info("Dynamic balancer assigned job id:\t" + jobID + ".");
            BalancerLogger.logReceivedJob(jobID);
            BalancerLogger.logSentWorkerReq(workerID, jobID);
            worker.doWork(workerController, balancerRequest, callback);
        } catch (Exception ex) {
            log.error("Server exception:\t" + ex);
            ex.printStackTrace();
        }
    }

    protected void onResponse(final int workerID, final int jobID) {
        BalancerLogger.logReceivedWorkerResp(workerID, jobID);
    }

    protected abstract int newWorkerID();
}
