package com.loadbalancers.balancer.impl.roundrobin;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.loadbalancers.balancer.impl.DynamicLoadBalancerImpl;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 20/April/2015
 */
public class RoundRobinLoadBalancerImpl extends DynamicLoadBalancerImpl {
    public RoundRobinLoadBalancerImpl(List<PeerInfo> servers, String localHostname, int localPort) {
        super(servers, localHostname, localPort);
    }

    @Override
    protected int newWorkerID() {
        if (workerIdToChannelMap.isEmpty()) {
            throw new IllegalStateException("Error: no workers available!");
        } else {
            if (nextServer >= workerIdToChannelMap.size()) {
                nextServer = 0;
            }
            return nextServer++;
        }
    }
}
