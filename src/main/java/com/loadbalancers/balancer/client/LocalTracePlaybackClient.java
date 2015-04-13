package com.loadbalancers.balancer.client;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.rpc.client.RpcClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class LocalTracePlaybackClient extends RpcClient {
    private final static Logger log = LogManager.getLogger(LocalTracePlaybackClient.class);
    protected final TreeSet<Integer> outstandingJobIDs = new TreeSet<>();

    protected final RpcClientChannel serverChannel;
    protected final ClientRpcController serverController;
    protected final LoadBalancer.LoadBalancerServer.Interface nonBlockingService;

    protected int currentID = 0;

    public LocalTracePlaybackClient (final PeerInfo server,
                                     final String hostname,
                                     final int localport) {
        super(Arrays.asList(server), hostname, localport);

        serverChannel = channels.get(server);

        nonBlockingService = LoadBalancer.LoadBalancerServer.newStub(serverChannel);
        serverController = serverChannel.newRpcController();
        serverController.setTimeoutMs(0);
    }

    public void runTrace (final LoadBalancer.Trace trace) {
        log.info("Running trace:\t" + trace.getTraceName());
        final List<LoadBalancer.TraceRequest> reqs = trace.getReqsList();
        reqs.forEach(q -> {
            final long waitTime = q.getInterarrivalDelay();
            Uninterruptibles.sleepUninterruptibly(waitTime, TimeUnit.MILLISECONDS);
            sendNonblockingRequest(q.getReq());
        });

        while (outstandingJobIDs.size() > 0) {
            log.info("Waiting on " + outstandingJobIDs.size() + " outstanding jobs...");
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        }
        log.info("Done with trace!");
    }

    protected void sendNonblockingRequest(final LoadBalancer.ClientRequest req) {
        try {
            final int jobID = currentID++;
            outstandingJobIDs.add(jobID);
            final long startTime = System.currentTimeMillis();

            RpcCallback<LoadBalancer.ClientResponse> callback = res -> {
                final long endTime = System.currentTimeMillis();
                final long delta = endTime - startTime;
                log.info("Job " + jobID + " finished in " + delta + " ms.");
                outstandingJobIDs.remove(jobID);
            };

            nonBlockingService.makeRequest(serverController, req, callback);
        } catch ( Throwable t) {
            log.error("Something went wrong making a nonblocking request:\t" + t, t);
        }
    }
}
