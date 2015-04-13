package com.loadbalancers.balancer.impl.random;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.balancer.impl.LoadBalancerImpl;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class RandomLoadBalancerImpl extends LoadBalancerImpl {

    private final static Logger log = LogManager.getLogger(RandomLoadBalancerImpl.class);
    protected AtomicInteger nextJobID;

    public RandomLoadBalancerImpl(final List<PeerInfo> servers,
                                  final String localHostname,
                                  final int localPort) {
        super(servers, localHostname, localPort);
        nextJobID = new AtomicInteger(0);
    }

    @Override
    public void makeRequest(final RpcController controller,
                            final LoadBalancer.ClientRequest request,
                            final RpcCallback<LoadBalancer.ClientResponse> done) {
        try {
            log.info("Random load balancer received message:\t" + request);
            final RpcClientChannel randomServer = getRandomServer();
            final LoadBalancer.LoadBalancerWorker.Stub worker = LoadBalancer.LoadBalancerWorker.newStub(randomServer);
            final ClientRpcController workerController = randomServer.newRpcController();
            workerController.setTimeoutMs(0);

            final RpcCallback<LoadBalancer.BalancerResponse> callback = res -> {
                final LoadBalancer.ClientResponse.Builder respBuilder = LoadBalancer.ClientResponse.newBuilder();

                respBuilder.setResponse(res.getResponse());

                final LoadBalancer.ClientResponse resp = respBuilder.build();
                done.run(resp);
            };

            final LoadBalancer.BalancerRequest balancerRequest = buildWorkerRequest(request);
            worker.doWork(workerController, balancerRequest, callback);
        } catch (Exception ex) {
            log.error("Server exception:\t" + ex);
            ex.printStackTrace();
        }
    }
    protected LoadBalancer.BalancerRequest buildWorkerRequest (final LoadBalancer.ClientRequest clientReq) {
        LoadBalancer.BalancerRequest.Builder reqBuilder = LoadBalancer.BalancerRequest.newBuilder();
        reqBuilder.setJobID(nextJobID.getAndIncrement());
        reqBuilder.setType(clientReq.getType());
        if (clientReq.hasSimulatedJobDuration()) {
            reqBuilder.setSimulatedJobDuration(clientReq.getSimulatedJobDuration());
        }
        return reqBuilder.build();
    }
}
