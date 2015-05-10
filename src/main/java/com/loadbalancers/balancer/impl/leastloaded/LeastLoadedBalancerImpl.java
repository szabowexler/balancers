package com.loadbalancers.balancer.impl.leastloaded;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.balancer.impl.DynamicLoadBalancerImpl;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Elias Szabo-Wexler
 * @since 20/April/2015
 */
public class LeastLoadedBalancerImpl extends DynamicLoadBalancerImpl {

    private final static Logger log = LogManager.getLogger(LeastLoadedBalancerImpl.class);
    protected final Map<Integer, Integer> workerLoadMap;

    public LeastLoadedBalancerImpl(final List<PeerInfo> servers,
                                   final String localHostname,
                                   final int localPort) {
        super(servers, localHostname, localPort);
        this.workerLoadMap = new ConcurrentHashMap<>();
        workerIdToChannelMap.keySet().forEach(id -> workerLoadMap.put(id, 0));
    }

    @Override
    public void makeRequest(final RpcController controller,
                            final LoadBalancer.ClientRequest request,
                            final RpcCallback<LoadBalancer.ClientResponse> done) {
        final int leastLoadedWorker = newWorkerID();
        doRequest(controller, request, done, leastLoadedWorker);
        workerLoadMap.put(leastLoadedWorker, workerLoadMap.get(leastLoadedWorker) + 1);
    }

    @Override
    protected void onResponse(int workerID, int jobID) {
        super.onResponse(workerID, jobID);
        workerLoadMap.put(workerID, workerLoadMap.get(workerID) - 1);
    }

    @Override
    protected int newWorkerID() {
        final Optional<Map.Entry<Integer, Integer>> minLoaded =
                workerLoadMap.entrySet().stream().min(Comparator.comparing(e -> e.getValue()));
        if (!minLoaded.isPresent()) {
            throw new IllegalStateException("Lacking a minimally loaded worker!");
        }
        return minLoaded.get().getKey();
    }
}
